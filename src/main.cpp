#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <mutex>
#include <numeric>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include <nlohmann/json.hpp>

namespace fs = std::filesystem;

namespace {

constexpr int64_t MS_PER_SEC = 1000LL;
constexpr int64_t MS_PER_MIN = 60LL * MS_PER_SEC;
constexpr int64_t MS_PER_HOUR = 60LL * MS_PER_MIN;
constexpr int64_t MS_PER_DAY = 24LL * MS_PER_HOUR;
constexpr int64_t SESSION_GAP_MS = 30LL * MS_PER_MIN;     // 1,800,000 ms
constexpr int64_t LATE_LIMIT_MS = 24LL * MS_PER_HOUR;     // 86,400,000 ms
constexpr int64_t LOOKBACK_MS = 7LL * MS_PER_DAY;         // 604,800,000 ms

inline bool is_ascii_digit(char c) noexcept {
    return c >= '0' && c <= '9';
}

inline int digit_val(char c) noexcept {
    return c - '0';
}

inline bool is_leap_year(int year) noexcept {
    return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
}

inline int days_in_month(int year, int month) noexcept {
    static constexpr int kDays[13] = {
        0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31
    };
    if (month == 2 && is_leap_year(year)) return 29;
    if (month < 1 || month > 12) return 0;
    return kDays[month];
}

// Days since 1970-01-01 (civil-from-days / days-from-civil algorithm, Howard Hinnant)
inline int64_t days_from_civil(int64_t y, unsigned m, unsigned d) noexcept {
    y -= (m <= 2) ? 1 : 0;
    const int64_t era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = static_cast<unsigned>(y - era * 400);              // [0, 399]
    const unsigned doy = (153 * (m > 2 ? m - 3 : m + 9) + 2) / 5 + d - 1;  // [0, 365]
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;             // [0, 146096]
    return era * 146097 + static_cast<int64_t>(doe) - 719468;
}

inline int32_t utc_day_from_ms(int64_t ms) noexcept {
    int64_t days = ms / MS_PER_DAY;
    int64_t rem = ms % MS_PER_DAY;
    if (rem < 0) {
        --days;
    }
    return static_cast<int32_t>(days);
}

inline bool parse_two_digits(std::string_view sv, size_t pos, int& out) noexcept {
    if (pos + 2 > sv.size()) return false;
    if (!is_ascii_digit(sv[pos]) || !is_ascii_digit(sv[pos + 1])) return false;
    out = digit_val(sv[pos]) * 10 + digit_val(sv[pos + 1]);
    return true;
}

inline bool parse_four_digits(std::string_view sv, size_t pos, int& out) noexcept {
    if (pos + 4 > sv.size()) return false;
    for (size_t i = 0; i < 4; ++i) {
        if (!is_ascii_digit(sv[pos + i])) return false;
    }
    out = digit_val(sv[pos]) * 1000 +
          digit_val(sv[pos + 1]) * 100 +
          digit_val(sv[pos + 2]) * 10 +
          digit_val(sv[pos + 3]);
    return true;
}

// Parses F1: YYYY-MM-DDTHH:MM:SSZ
// or    F2: YYYY-MM-DDTHH:MM:SS+HH:MM / -HH:MM
// or    F3: ASCII digits only (epoch ms, leading zeros allowed)
bool parse_timestamp_ms(std::string_view s, int64_t& out_ms) noexcept {
    if (s.empty()) return false;

    bool all_digits = true;
    for (char c : s) {
        if (!is_ascii_digit(c)) {
            all_digits = false;
            break;
        }
    }

    if (all_digits) {
        size_t i = 0;
        while (i < s.size() && s[i] == '0') ++i;
        std::string_view sig = s.substr(i);
        if (sig.empty()) {
            out_ms = 0;
            return true;
        }
        int64_t val = 0;
        auto [ptr, ec] = std::from_chars(sig.data(), sig.data() + sig.size(), val);
        if (ec != std::errc{} || ptr != sig.data() + sig.size()) {
            // Out of int64 range -> not malformed per spec, outside window
            out_ms = std::numeric_limits<int64_t>::max();
            return true;
        }
        out_ms = val;
        return true;
    }

    // F1 length = 20, F2 length = 25
    if (s.size() != 20 && s.size() != 25) return false;
    if (s[4] != '-' || s[7] != '-' || s[10] != 'T' || s[13] != ':' || s[16] != ':') {
        return false;
    }

    int year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0;
    if (!parse_four_digits(s, 0, year)) return false;
    if (!parse_two_digits(s, 5, month)) return false;
    if (!parse_two_digits(s, 8, day)) return false;
    if (!parse_two_digits(s, 11, hour)) return false;
    if (!parse_two_digits(s, 14, minute)) return false;
    if (!parse_two_digits(s, 17, second)) return false;

    if (month < 1 || month > 12) return false;
    int max_d = days_in_month(year, month);
    if (day < 1 || day > max_d) return false;
    if (hour < 0 || hour > 23) return false;
    if (minute < 0 || minute > 59) return false;
    if (second < 0 || second > 59) return false;

    int64_t days = days_from_civil(year, static_cast<unsigned>(month), static_cast<unsigned>(day));
    int64_t local_sec = days * 86400LL +
                        static_cast<int64_t>(hour) * 3600LL +
                        static_cast<int64_t>(minute) * 60LL +
                        static_cast<int64_t>(second);

    if (s.size() == 20) {
        if (s[19] != 'Z') return false;
        out_ms = local_sec * MS_PER_SEC;
        return true;
    }

    // F2 offset: +HH:MM or -HH:MM
    char sign = s[19];
    if (sign != '+' && sign != '-') return false;
    if (s[22] != ':') return false;
    int off_h = 0, off_m = 0;
    if (!parse_two_digits(s, 20, off_h)) return false;
    if (!parse_two_digits(s, 23, off_m)) return false;
    if (off_h < 0 || off_h > 23 || off_m < 0 || off_m > 59) return false;

    int64_t offset_sec = static_cast<int64_t>(off_h) * 3600LL + static_cast<int64_t>(off_m) * 60LL;
    if (sign == '+') {
        out_ms = (local_sec - offset_sec) * MS_PER_SEC;
    } else {
        out_ms = (local_sec + offset_sec) * MS_PER_SEC;
    }
    return true;
}

// Parses a single CSV line under rules.md dialect:
// - Empty line -> malformed
// - Only '\n' splits lines; '\r' is ordinary character
// - Quoted field must start with '"' at index 0 of the field and end with '"' immediately before ',' or end-of-line
// - Quotes anywhere else -> malformed
bool parse_csv_line(std::string_view line, size_t expected_cols, std::vector<std::string_view>& out_fields) {
    out_fields.clear();
    if (line.empty()) return false;
    out_fields.reserve(expected_cols);

    size_t pos = 0;
    const size_t n = line.size();

    while (true) {
        if (pos < n && line[pos] == '"') {
            // Quoted field
            size_t start = pos + 1;
            size_t end_quote = start;
            while (end_quote < n && line[end_quote] != '"') {
                ++end_quote;
            }
            if (end_quote >= n || line[end_quote] != '"') {
                return false; // unclosed quote
            }
            size_t after = end_quote + 1;
            if (after == n) {
                out_fields.emplace_back(line.substr(start, end_quote - start));
                break;
            } else if (line[after] == ',') {
                out_fields.emplace_back(line.substr(start, end_quote - start));
                pos = after + 1;
                // If line ends right after comma, there is one more empty field
                if (pos == n) {
                    out_fields.emplace_back(line.substr(n, 0));
                    break;
                }
            } else {
                // Characters after closing quote before comma
                return false;
            }
        } else {
            // Unquoted field
            size_t start = pos;
            size_t cur = pos;
            while (cur < n && line[cur] != ',') {
                if (line[cur] == '"') {
                    return false; // quote inside unquoted field
                }
                ++cur;
            }
            out_fields.emplace_back(line.substr(start, cur - start));
            if (cur == n) {
                break;
            }
            pos = cur + 1;
            if (pos == n) {
                out_fields.emplace_back(line.substr(n, 0));
                break;
            }
        }
    }

    return out_fields.size() == expected_cols;
}

bool parse_price_cents(std::string_view s, int64_t& out_price) noexcept {
    if (s.empty()) return false;
    for (char c : s) {
        if (!is_ascii_digit(c)) return false;
    }
    size_t i = 0;
    while (i < s.size() && s[i] == '0') ++i;
    std::string_view sig = s.substr(i);
    if (sig.empty()) return false; // 0 is not allowed (must be 1..10,000,000)

    int64_t val = 0;
    auto [ptr, ec] = std::from_chars(sig.data(), sig.data() + sig.size(), val);
    if (ec != std::errc{} || ptr != sig.data() + sig.size()) return false;
    if (val < 1LL || val > 10000000LL) return false;

    out_price = val;
    return true;
}

std::string read_file_bytes(const fs::path& p) {
    std::ifstream ifs(p, std::ios::binary | std::ios::ate);
    if (!ifs) {
        throw std::runtime_error("Failed to open file: " + p.string());
    }
    const auto sz = ifs.tellg();
    std::string buf(static_cast<size_t>(sz), '\0');
    ifs.seekg(0);
    if (sz > 0) {
        ifs.read(buf.data(), sz);
    }
    return buf;
}

std::vector<fs::path> list_csv_files(const fs::path& dir) {
    std::vector<fs::path> files;
    if (!fs::exists(dir)) return files;
    for (const auto& entry : fs::directory_iterator(dir)) {
        if (!entry.is_regular_file() && !entry.is_symlink()) continue;
        const auto p = entry.path();
        if (p.extension() == ".csv") {
            files.push_back(p);
        }
    }
    std::sort(files.begin(), files.end());
    return files;
}

template <typename Fn>
void for_each_csv_line(std::string_view data, Fn&& fn) {
    size_t pos = 0;
    const size_t n = data.size();
    bool first_line = true;
    while (pos < n) {
        size_t nl = data.find('\n', pos);
        std::string_view line;
        if (nl == std::string_view::npos) {
            line = data.substr(pos);
            pos = n;
        } else {
            line = data.substr(pos, nl - pos);
            pos = nl + 1;
        }
        if (first_line) {
            first_line = false;
            continue; // skip header row
        }
        fn(line);
    }
}

struct DSU {
    std::vector<uint32_t> parent;
    std::vector<uint8_t> rank;
    std::unordered_map<std::string, uint32_t> id_to_idx;
    std::vector<std::string> idx_to_id;

    uint32_t get_or_add(std::string_view id) {
        auto it = id_to_idx.find(std::string(id));
        if (it != id_to_idx.end()) return it->second;
        uint32_t idx = static_cast<uint32_t>(idx_to_id.size());
        idx_to_id.emplace_back(id);
        parent.push_back(idx);
        rank.push_back(0);
        id_to_idx.emplace(idx_to_id.back(), idx);
        return idx;
    }

    std::optional<uint32_t> find_existing(std::string_view id) const {
        auto it = id_to_idx.find(std::string(id));
        if (it == id_to_idx.end()) return std::nullopt;
        return it->second;
    }

    uint32_t find_root(uint32_t x) {
        while (parent[x] != x) {
            parent[x] = parent[parent[x]];
            x = parent[x];
        }
        return x;
    }

    void unite(uint32_t a, uint32_t b) {
        uint32_t ra = find_root(a);
        uint32_t rb = find_root(b);
        if (ra == rb) return;
        if (rank[ra] < rank[rb]) std::swap(ra, rb);
        parent[rb] = ra;
        if (rank[ra] == rank[rb]) ++rank[ra];
    }
};

struct RawEvent {
    std::string event_id;
    std::string user_id;
    std::string ts_str;
    int64_t ingest_ts_ms = 0;
    std::string event_type;
    std::string product_id;
    std::string price_str;
    bool malformed = false;
};

struct CleanEvent {
    uint32_t person_id = 0;
    int64_t ts_ms = 0;
    uint8_t type = 0; // 1=view, 2=cart, 3=purchase
    uint32_t product_idx = 0;
    int64_t price_cents = 0;
};

struct ValidClick {
    uint32_t person_id = 0;
    int64_t ts_ms = 0;
    std::string click_id;
    uint32_t campaign_idx = 0;
};

struct SessionRecord {
    int64_t start_ms = 0;
    int32_t day = 0;
    int64_t events = 0;
    int64_t revenue_cents = 0;
    bool has_purchase = false;
    std::unordered_map<uint32_t, int64_t> cat_revenue;
};

struct UserDailyRow {
    std::string user_id;
    int32_t day = 0;
    int64_t sessions = 0;
    int64_t events = 0;
    int64_t revenue_cents = 0;
    std::optional<std::string> top_category;
};

struct CampaignDailyKey {
    uint32_t campaign_idx = 0; // 0 reserved for "organic"
    int32_t day = 0;
    bool operator==(const CampaignDailyKey& o) const noexcept {
        return campaign_idx == o.campaign_idx && day == o.day;
    }
};

struct CampaignDailyKeyHash {
    size_t operator()(const CampaignDailyKey& k) const noexcept {
        return (static_cast<size_t>(k.campaign_idx) * 1315423911ULL) ^
               static_cast<size_t>(static_cast<uint32_t>(k.day));
    }
};

struct CampaignDailyAgg {
    int64_t sessions = 0;
    int64_t purchasing_sessions = 0;
    int64_t revenue_cents = 0;
    int64_t assisted_sessions = 0;
};

arrow::Status write_user_daily_parquet(const fs::path& out_dir, const std::vector<UserDailyRow>& rows) {
    fs::create_directories(out_dir);
    const fs::path out_file = out_dir / "part-0.parquet";

    arrow::StringBuilder user_id_b;
    arrow::Date32Builder day_b;
    arrow::Int64Builder sessions_b;
    arrow::Int64Builder events_b;
    arrow::Int64Builder revenue_b;
    arrow::StringBuilder top_cat_b;

    ARROW_RETURN_NOT_OK(user_id_b.Reserve(static_cast<int64_t>(rows.size())));
    ARROW_RETURN_NOT_OK(day_b.Reserve(static_cast<int64_t>(rows.size())));
    ARROW_RETURN_NOT_OK(sessions_b.Reserve(static_cast<int64_t>(rows.size())));
    ARROW_RETURN_NOT_OK(events_b.Reserve(static_cast<int64_t>(rows.size())));
    ARROW_RETURN_NOT_OK(revenue_b.Reserve(static_cast<int64_t>(rows.size())));
    ARROW_RETURN_NOT_OK(top_cat_b.Reserve(static_cast<int64_t>(rows.size())));

    for (const auto& r : rows) {
        ARROW_RETURN_NOT_OK(user_id_b.Append(r.user_id));
        ARROW_RETURN_NOT_OK(day_b.Append(r.day));
        ARROW_RETURN_NOT_OK(sessions_b.Append(r.sessions));
        ARROW_RETURN_NOT_OK(events_b.Append(r.events));
        ARROW_RETURN_NOT_OK(revenue_b.Append(r.revenue_cents));
        if (r.top_category.has_value()) {
            ARROW_RETURN_NOT_OK(top_cat_b.Append(*r.top_category));
        } else {
            ARROW_RETURN_NOT_OK(top_cat_b.AppendNull());
        }
    }

    std::shared_ptr<arrow::Array> user_id_arr, day_arr, sessions_arr, events_arr, revenue_arr, top_cat_arr;
    ARROW_RETURN_NOT_OK(user_id_b.Finish(&user_id_arr));
    ARROW_RETURN_NOT_OK(day_b.Finish(&day_arr));
    ARROW_RETURN_NOT_OK(sessions_b.Finish(&sessions_arr));
    ARROW_RETURN_NOT_OK(events_b.Finish(&events_arr));
    ARROW_RETURN_NOT_OK(revenue_b.Finish(&revenue_arr));
    ARROW_RETURN_NOT_OK(top_cat_b.Finish(&top_cat_arr));

    auto schema = arrow::schema({
        arrow::field("user_id", arrow::utf8(), false),
        arrow::field("day", arrow::date32(), false),
        arrow::field("sessions", arrow::int64(), false),
        arrow::field("events", arrow::int64(), false),
        arrow::field("revenue_cents", arrow::int64(), false),
        arrow::field("top_category", arrow::utf8(), true),
    });

    auto table = arrow::Table::Make(schema, {
        user_id_arr, day_arr, sessions_arr, events_arr, revenue_arr, top_cat_arr
    });

    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(out_file.string()));

    auto props = parquet::WriterProperties::Builder()
        ->compression(parquet::Compression::SNAPPY)
        ->build();

    return parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, 1048576, props);
}

arrow::Status write_campaign_daily_parquet(
    const fs::path& out_dir,
    const std::unordered_map<CampaignDailyKey, CampaignDailyAgg, CampaignDailyKeyHash>& agg_map,
    const std::vector<std::string>& campaign_ids,
    const std::vector<std::string>& campaign_channels) {
    fs::create_directories(out_dir);
    const fs::path out_file = out_dir / "part-0.parquet";

    const size_t n = agg_map.size();
    arrow::StringBuilder camp_id_b;
    arrow::StringBuilder channel_b;
    arrow::Date32Builder day_b;
    arrow::Int64Builder sessions_b;
    arrow::Int64Builder purch_b;
    arrow::Int64Builder rev_b;
    arrow::Int64Builder assisted_b;

    ARROW_RETURN_NOT_OK(camp_id_b.Reserve(static_cast<int64_t>(n)));
    ARROW_RETURN_NOT_OK(channel_b.Reserve(static_cast<int64_t>(n)));
    ARROW_RETURN_NOT_OK(day_b.Reserve(static_cast<int64_t>(n)));
    ARROW_RETURN_NOT_OK(sessions_b.Reserve(static_cast<int64_t>(n)));
    ARROW_RETURN_NOT_OK(purch_b.Reserve(static_cast<int64_t>(n)));
    ARROW_RETURN_NOT_OK(rev_b.Reserve(static_cast<int64_t>(n)));
    ARROW_RETURN_NOT_OK(assisted_b.Reserve(static_cast<int64_t>(n)));

    for (const auto& [k, v] : agg_map) {
        ARROW_RETURN_NOT_OK(camp_id_b.Append(campaign_ids[k.campaign_idx]));
        ARROW_RETURN_NOT_OK(channel_b.Append(campaign_channels[k.campaign_idx]));
        ARROW_RETURN_NOT_OK(day_b.Append(k.day));
        ARROW_RETURN_NOT_OK(sessions_b.Append(v.sessions));
        ARROW_RETURN_NOT_OK(purch_b.Append(v.purchasing_sessions));
        ARROW_RETURN_NOT_OK(rev_b.Append(v.revenue_cents));
        ARROW_RETURN_NOT_OK(assisted_b.Append(v.assisted_sessions));
    }

    std::shared_ptr<arrow::Array> camp_id_arr, channel_arr, day_arr, sessions_arr, purch_arr, rev_arr, assisted_arr;
    ARROW_RETURN_NOT_OK(camp_id_b.Finish(&camp_id_arr));
    ARROW_RETURN_NOT_OK(channel_b.Finish(&channel_arr));
    ARROW_RETURN_NOT_OK(day_b.Finish(&day_arr));
    ARROW_RETURN_NOT_OK(sessions_b.Finish(&sessions_arr));
    ARROW_RETURN_NOT_OK(purch_b.Finish(&purch_arr));
    ARROW_RETURN_NOT_OK(rev_b.Finish(&rev_arr));
    ARROW_RETURN_NOT_OK(assisted_b.Finish(&assisted_arr));

    auto schema = arrow::schema({
        arrow::field("campaign_id", arrow::utf8(), false),
        arrow::field("channel", arrow::utf8(), false),
        arrow::field("day", arrow::date32(), false),
        arrow::field("sessions", arrow::int64(), false),
        arrow::field("purchasing_sessions", arrow::int64(), false),
        arrow::field("revenue_cents", arrow::int64(), false),
        arrow::field("assisted_sessions", arrow::int64(), false),
    });

    auto table = arrow::Table::Make(schema, {
        camp_id_arr, channel_arr, day_arr, sessions_arr, purch_arr, rev_arr, assisted_arr
    });

    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(out_file.string()));

    auto props = parquet::WriterProperties::Builder()
        ->compression(parquet::Compression::SNAPPY)
        ->build();

    return parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, 1048576, props);
}

} // namespace

int main() {
    try {
        const char* env_data = std::getenv("DATA_DIR");
        const char* env_out = std::getenv("OUTPUT_DIR");
        const char* env_ncpu = std::getenv("NCPU");

        const fs::path data_dir = env_data ? env_data : "/data";
        const fs::path out_dir = env_out ? env_out : "/output";
        int ncpu = 8;
        if (env_ncpu) {
            int parsed = std::atoi(env_ncpu);
            if (parsed > 0) ncpu = parsed;
        }
        const size_t num_threads = static_cast<size_t>(std::max(1, ncpu));

        // 1. Read manifest.json
        int64_t window_start_ms = 0;
        int64_t window_end_ms = 0;
        {
            std::string manifest_str = read_file_bytes(data_dir / "manifest.json");
            auto j = nlohmann::json::parse(manifest_str);
            std::string ws_s = j.at("window_start").get<std::string>();
            std::string we_s = j.at("window_end").get<std::string>();
            if (!parse_timestamp_ms(ws_s, window_start_ms) || !parse_timestamp_ms(we_s, window_end_ms)) {
                throw std::runtime_error("Invalid manifest window timestamps");
            }
        }
        const int64_t click_window_start_ms = window_start_ms - LOOKBACK_MS;

        // 2. Read products.csv and categories
        // Index 0 reserved for "unknown"
        std::vector<std::string> categories;
        categories.push_back("unknown");
        std::unordered_map<std::string, uint32_t> category_to_idx;
        category_to_idx["unknown"] = 0;

        std::unordered_map<std::string, uint32_t> product_to_cat_idx;
        {
            std::string prod_bytes = read_file_bytes(data_dir / "products.csv");
            std::vector<std::string_view> fields;
            for_each_csv_line(prod_bytes, [&](std::string_view line) {
                if (!parse_csv_line(line, 2, fields)) return;
                std::string pid(fields[0]);
                std::string cat(fields[1]);
                uint32_t cidx = 0;
                auto cit = category_to_idx.find(cat);
                if (cit == category_to_idx.end()) {
                    cidx = static_cast<uint32_t>(categories.size());
                    categories.push_back(cat);
                    category_to_idx.emplace(categories.back(), cidx);
                } else {
                    cidx = cit->second;
                }
                product_to_cat_idx.emplace(std::move(pid), cidx);
            });
        }

        // 3. Read campaigns.csv
        // Index 0 reserved for "organic"
        std::vector<std::string> campaign_ids;
        std::vector<std::string> campaign_channels;
        campaign_ids.push_back("organic");
        campaign_channels.push_back("organic");
        std::unordered_map<std::string, uint32_t> campaign_to_idx;
        campaign_to_idx["organic"] = 0;

        {
            std::string camp_bytes = read_file_bytes(data_dir / "campaigns.csv");
            std::vector<std::string_view> fields;
            for_each_csv_line(camp_bytes, [&](std::string_view line) {
                if (!parse_csv_line(line, 2, fields)) return;
                std::string cid(fields[0]);
                std::string ch(fields[1]);
                uint32_t idx = static_cast<uint32_t>(campaign_ids.size());
                campaign_ids.push_back(cid);
                campaign_channels.push_back(std::move(ch));
                campaign_to_idx.emplace(std::move(cid), idx);
            });
        }

        // 4. Read links/*.csv and build DSU + canonical person names
        const auto link_files = list_csv_files(data_dir / "links");
        std::vector<std::string> link_buffers(link_files.size());
        for (size_t i = 0; i < link_files.size(); ++i) {
            link_buffers[i] = read_file_bytes(link_files[i]);
        }

        DSU dsu;
        {
            std::vector<std::string_view> fields;
            for (const auto& buf : link_buffers) {
                for_each_csv_line(buf, [&](std::string_view line) {
                    if (!parse_csv_line(line, 2, fields)) return;
                    if (fields[0].empty() || fields[1].empty()) return;
                    uint32_t a = dsu.get_or_add(fields[0]);
                    uint32_t b = dsu.get_or_add(fields[1]);
                    dsu.unite(a, b);
                });
            }
        }

        // Map each DSU root to smallest string ID in component
        const size_t num_dsu_nodes = dsu.idx_to_id.size();
        std::unordered_map<uint32_t, std::string> root_to_min_name;
        root_to_min_name.reserve(num_dsu_nodes / 2 + 16);
        for (uint32_t i = 0; i < num_dsu_nodes; ++i) {
            uint32_t r = dsu.find_root(i);
            const std::string& cand = dsu.idx_to_id[i];
            auto it = root_to_min_name.find(r);
            if (it == root_to_min_name.end()) {
                root_to_min_name.emplace(r, cand);
            } else if (cand < it->second) {
                it->second = cand;
            }
        }

        std::unordered_map<std::string, uint32_t> person_name_to_id;
        std::vector<std::string> person_id_to_name;
        person_name_to_id.reserve(root_to_min_name.size() + 65536);
        person_id_to_name.reserve(root_to_min_name.size() + 65536);

        auto get_or_create_person_by_name = [&](const std::string& name) -> uint32_t {
            auto it = person_name_to_id.find(name);
            if (it != person_name_to_id.end()) return it->second;
            uint32_t pid = static_cast<uint32_t>(person_id_to_name.size());
            person_id_to_name.push_back(name);
            person_name_to_id.emplace(person_id_to_name.back(), pid);
            return pid;
        };

        // Pre-populate persons that exist purely in links/
        for (const auto& [r, min_name] : root_to_min_name) {
            (void)r;
            get_or_create_person_by_name(min_name);
        }

        std::unordered_map<uint32_t, uint32_t> dsu_node_to_person;
        dsu_node_to_person.reserve(num_dsu_nodes);
        for (uint32_t i = 0; i < num_dsu_nodes; ++i) {
            uint32_t r = dsu.find_root(i);
            uint32_t pid = get_or_create_person_by_name(root_to_min_name[r]);
            dsu_node_to_person.emplace(i, pid);
        }

        std::mutex person_mtx;
        auto resolve_user_id = [&](std::string_view uid) -> uint32_t {
            auto dsu_idx = dsu.find_existing(uid);
            if (dsu_idx.has_value()) {
                return dsu_node_to_person.at(*dsu_idx);
            }
            std::string s(uid);
            std::lock_guard<std::mutex> lk(person_mtx);
            return get_or_create_person_by_name(s);
        };

        // 5. Read & parse events/*.csv in parallel
        const auto event_files = list_csv_files(data_dir / "events");
        std::vector<std::string> event_buffers(event_files.size());
        for (size_t i = 0; i < event_files.size(); ++i) {
            event_buffers[i] = read_file_bytes(event_files[i]);
        }

        struct FileEvents {
            std::vector<RawEvent> items;
        };
        std::vector<FileEvents> per_file_events(event_files.size());

        {
            std::atomic<size_t> next_idx{0};
            std::vector<std::thread> workers;
            size_t n_w = std::min(num_threads, event_files.size());
            for (size_t t = 0; t < n_w; ++t) {
                workers.emplace_back([&]() {
                    while (true) {
                        size_t idx = next_idx.fetch_add(1, std::memory_order_relaxed);
                        if (idx >= event_files.size()) break;
                        auto& out = per_file_events[idx].items;
                        std::vector<std::string_view> fields;
                        for_each_csv_line(event_buffers[idx], [&](std::string_view line) {
                            RawEvent ev;
                            if (!parse_csv_line(line, 7, fields)) {
                                ev.malformed = true;
                                out.push_back(std::move(ev));
                                return;
                            }
                            if (fields[0].empty()) {
                                ev.malformed = true;
                                out.push_back(std::move(ev));
                                return;
                            }
                            int64_t ing_ms = 0;
                            if (!parse_timestamp_ms(fields[3], ing_ms)) {
                                ev.malformed = true;
                                out.push_back(std::move(ev));
                                return;
                            }
                            ev.event_id = std::string(fields[0]);
                            ev.user_id = std::string(fields[1]);
                            ev.ts_str = std::string(fields[2]);
                            ev.ingest_ts_ms = ing_ms;
                            ev.event_type = std::string(fields[4]);
                            ev.product_id = std::string(fields[5]);
                            ev.price_str = std::string(fields[6]);
                            out.push_back(std::move(ev));
                        });
                    }
                });
            }
            for (auto& w : workers) w.join();
        }
        // Release raw CSV buffers once parsed into structs
        event_buffers.clear();
        event_buffers.shrink_to_fit();

        // Dedup events on event_id by smallest ingest_ts BEFORE validation
        std::unordered_map<std::string, RawEvent> kept_events;
        size_t total_raw_events = 0;
        for (const auto& fe : per_file_events) total_raw_events += fe.items.size();
        kept_events.reserve(total_raw_events);

        for (auto& fe : per_file_events) {
            for (auto& ev : fe.items) {
                if (ev.malformed) continue;
                std::string key = ev.event_id;
                auto it = kept_events.find(key);
                if (it == kept_events.end()) {
                    kept_events.emplace(std::move(key), std::move(ev));
                } else {
                    if (ev.ingest_ts_ms < it->second.ingest_ts_ms) {
                        it->second = std::move(ev);
                    }
                }
            }
        }
        per_file_events.clear();
        per_file_events.shrink_to_fit();

        // Validate kept events & apply late filter
        std::vector<CleanEvent> clean_events;
        clean_events.reserve(kept_events.size());

        for (const auto& [eid, ev] : kept_events) {
            (void)eid;
            if (ev.malformed) continue;
            if (ev.user_id.empty()) continue;
            if (ev.product_id.empty()) continue;

            uint8_t etype = 0;
            if (ev.event_type == "view") etype = 1;
            else if (ev.event_type == "cart") etype = 2;
            else if (ev.event_type == "purchase") etype = 3;
            else continue;

            int64_t ts_ms = 0;
            if (!parse_timestamp_ms(ev.ts_str, ts_ms)) continue;
            if (ts_ms < window_start_ms || ts_ms >= window_end_ms) continue;

            int64_t price_cents = 0;
            if (etype == 3) {
                if (!parse_price_cents(ev.price_str, price_cents)) continue;
            }

            // Late event check: drop if ingest_ts - ts > 24 hours (86,400,000 ms)
            if ((ev.ingest_ts_ms - ts_ms) > LATE_LIMIT_MS) continue;

            uint32_t cat_idx = 0;
            auto pit = product_to_cat_idx.find(ev.product_id);
            if (pit != product_to_cat_idx.end()) {
                cat_idx = pit->second;
            }

            uint32_t pid = resolve_user_id(ev.user_id);
            clean_events.push_back(CleanEvent{
                pid, ts_ms, etype, cat_idx, price_cents
            });
        }
        kept_events.clear();

        // 6. Read, dedup, and validate clicks/*.csv
        const auto click_files = list_csv_files(data_dir / "clicks");
        std::vector<std::string> click_buffers(click_files.size());
        for (size_t i = 0; i < click_files.size(); ++i) {
            click_buffers[i] = read_file_bytes(click_files[i]);
        }

        std::unordered_set<std::string> seen_click_ids;
        seen_click_ids.reserve(1000000);
        std::vector<ValidClick> valid_clicks;
        valid_clicks.reserve(1000000);

        {
            std::vector<std::string_view> fields;
            for (const auto& buf : click_buffers) {
                for_each_csv_line(buf, [&](std::string_view line) {
                    if (!parse_csv_line(line, 4, fields)) return;
                    if (fields[0].empty()) return; // click_id empty
                    std::string cid(fields[0]);
                    if (!seen_click_ids.insert(cid).second) {
                        return; // duplicate click_id
                    }
                    if (fields[1].empty()) return; // user_id empty
                    auto cit = campaign_to_idx.find(std::string(fields[2]));
                    if (cit == campaign_to_idx.end()) return; // unknown campaign

                    int64_t ts_ms = 0;
                    if (!parse_timestamp_ms(fields[3], ts_ms)) return;
                    if (ts_ms < click_window_start_ms || ts_ms >= window_end_ms) return;

                    uint32_t pid = resolve_user_id(fields[1]);
                    valid_clicks.push_back(ValidClick{
                        pid, ts_ms, std::move(cid), cit->second
                    });
                });
            }
        }
        click_buffers.clear();
        click_buffers.shrink_to_fit();
        seen_click_ids.clear();

        // Group events and clicks by person_id
        const size_t num_persons = person_id_to_name.size();
        std::vector<std::vector<CleanEvent>> events_by_person(num_persons);
        for (auto& ce : clean_events) {
            events_by_person[ce.person_id].push_back(std::move(ce));
        }
        clean_events.clear();
        clean_events.shrink_to_fit();

        std::vector<std::vector<ValidClick>> clicks_by_person(num_persons);
        for (auto& vc : valid_clicks) {
            clicks_by_person[vc.person_id].push_back(std::move(vc));
        }
        valid_clicks.clear();
        valid_clicks.shrink_to_fit();

        // 7. Sessionize, attribute, compute assists, and aggregate per person in parallel
        struct PersonResult {
            std::vector<UserDailyRow> user_rows;
            std::unordered_map<CampaignDailyKey, CampaignDailyAgg, CampaignDailyKeyHash> camp_agg;
        };
        std::vector<PersonResult> person_results(num_persons);

        {
            std::atomic<size_t> next_p{0};
            std::vector<std::thread> workers;
            size_t n_w = std::min(num_threads, num_persons);
            for (size_t t = 0; t < n_w; ++t) {
                workers.emplace_back([&]() {
                    while (true) {
                        size_t p = next_p.fetch_add(1, std::memory_order_relaxed);
                        if (p >= num_persons) break;

                        auto& pevs = events_by_person[p];
                        if (pevs.empty()) continue;

                        std::sort(pevs.begin(), pevs.end(), [](const CleanEvent& a, const CleanEvent& b) {
                            return a.ts_ms < b.ts_ms;
                        });

                        auto& pclicks = clicks_by_person[p];
                        std::sort(pclicks.begin(), pclicks.end(), [](const ValidClick& a, const ValidClick& b) {
                            if (a.ts_ms != b.ts_ms) return a.ts_ms < b.ts_ms;
                            return a.click_id < b.click_id;
                        });

                        // Build sessions: new session when gap between distinct timestamps > 1,800,000 ms
                        std::vector<SessionRecord> sessions;
                        sessions.reserve(pevs.size() / 2 + 1);

                        SessionRecord cur;
                        cur.start_ms = pevs[0].ts_ms;
                        cur.day = utc_day_from_ms(pevs[0].ts_ms);
                        cur.events = 1;
                        if (pevs[0].type == 3) {
                            cur.has_purchase = true;
                            cur.revenue_cents += pevs[0].price_cents;
                            cur.cat_revenue[pevs[0].product_idx] += pevs[0].price_cents;
                        }

                        int64_t prev_distinct_ts = pevs[0].ts_ms;

                        for (size_t i = 1; i < pevs.size(); ++i) {
                            const auto& ev = pevs[i];
                            if (ev.ts_ms != prev_distinct_ts) {
                                if ((ev.ts_ms - prev_distinct_ts) > SESSION_GAP_MS) {
                                    sessions.push_back(std::move(cur));
                                    cur = SessionRecord{};
                                    cur.start_ms = ev.ts_ms;
                                    cur.day = utc_day_from_ms(ev.ts_ms);
                                }
                                prev_distinct_ts = ev.ts_ms;
                            }
                            cur.events += 1;
                            if (ev.type == 3) {
                                cur.has_purchase = true;
                                cur.revenue_cents += ev.price_cents;
                                cur.cat_revenue[ev.product_idx] += ev.price_cents;
                            }
                        }
                        sessions.push_back(std::move(cur));

                        auto& res = person_results[p];
                        const std::string& person_name = person_id_to_name[p];

                        // Group sessions by session day for user_daily
                        struct DayAgg {
                            int64_t sessions = 0;
                            int64_t events = 0;
                            int64_t revenue_cents = 0;
                            std::unordered_map<uint32_t, int64_t> cat_rev;
                        };
                        std::unordered_map<int32_t, DayAgg> day_map;

                        for (const auto& s : sessions) {
                            auto& da = day_map[s.day];
                            da.sessions += 1;
                            da.events += s.events;
                            da.revenue_cents += s.revenue_cents;
                            for (const auto& [cidx, crev] : s.cat_revenue) {
                                da.cat_rev[cidx] += crev;
                            }

                            // Attribution & assists using clicks with start_ms - 7d <= click.ts <= start_ms
                            const int64_t min_click_ts = s.start_ms - LOOKBACK_MS;
                            bool has_attr = false;
                            int64_t best_ts = std::numeric_limits<int64_t>::min();
                            std::string_view best_click_id;
                            uint32_t best_camp = 0;

                            // Collect unique assisting campaign indices (excluding organic = 0)
                            std::vector<uint32_t> assist_camps;

                            for (const auto& clk : pclicks) {
                                if (clk.ts_ms > s.start_ms) break;
                                if (clk.ts_ms < min_click_ts) continue;

                                assist_camps.push_back(clk.campaign_idx);

                                if (!has_attr ||
                                    clk.ts_ms > best_ts ||
                                    (clk.ts_ms == best_ts && clk.click_id < best_click_id)) {
                                    has_attr = true;
                                    best_ts = clk.ts_ms;
                                    best_click_id = clk.click_id;
                                    best_camp = clk.campaign_idx;
                                }
                            }

                            uint32_t credited_camp = has_attr ? best_camp : 0U;
                            auto& c_agg = res.camp_agg[CampaignDailyKey{credited_camp, s.day}];
                            c_agg.sessions += 1;
                            if (s.has_purchase) {
                                c_agg.purchasing_sessions += 1;
                                c_agg.revenue_cents += s.revenue_cents;
                            }

                            if (!assist_camps.empty()) {
                                std::sort(assist_camps.begin(), assist_camps.end());
                                assist_camps.erase(std::unique(assist_camps.begin(), assist_camps.end()), assist_camps.end());
                                for (uint32_t ac : assist_camps) {
                                    if (ac == 0U) continue; // organic never assists
                                    res.camp_agg[CampaignDailyKey{ac, s.day}].assisted_sessions += 1;
                                }
                            }
                        }

                        for (const auto& [day_val, da] : day_map) {
                            UserDailyRow ur;
                            ur.user_id = person_name;
                            ur.day = day_val;
                            ur.sessions = da.sessions;
                            ur.events = da.events;
                            ur.revenue_cents = da.revenue_cents;

                            if (!da.cat_rev.empty()) {
                                int64_t max_rev = -1;
                                std::string best_cat;
                                for (const auto& [cidx, crev] : da.cat_rev) {
                                    if (crev <= 0) continue;
                                    const std::string& cname = categories[cidx];
                                    if (crev > max_rev || (crev == max_rev && cname < best_cat)) {
                                        max_rev = crev;
                                        best_cat = cname;
                                    }
                                }
                                if (max_rev > 0) {
                                    ur.top_category = std::move(best_cat);
                                }
                            }
                            res.user_rows.push_back(std::move(ur));
                        }
                    }
                });
            }
            for (auto& w : workers) w.join();
        }

        // 8. Merge outputs across threads and write Parquet
        std::vector<UserDailyRow> all_user_rows;
        std::unordered_map<CampaignDailyKey, CampaignDailyAgg, CampaignDailyKeyHash> global_camp_agg;

        for (auto& pr : person_results) {
            all_user_rows.insert(
                all_user_rows.end(),
                std::make_move_iterator(pr.user_rows.begin()),
                std::make_move_iterator(pr.user_rows.end())
            );
            for (const auto& [k, v] : pr.camp_agg) {
                auto& dst = global_camp_agg[k];
                dst.sessions += v.sessions;
                dst.purchasing_sessions += v.purchasing_sessions;
                dst.revenue_cents += v.revenue_cents;
                dst.assisted_sessions += v.assisted_sessions;
            }
        }

        auto st1 = write_user_daily_parquet(out_dir / "user_daily", all_user_rows);
        if (!st1.ok()) {
            throw std::runtime_error("Failed writing user_daily parquet: " + st1.ToString());
        }

        auto st2 = write_campaign_daily_parquet(out_dir / "campaign_daily", global_camp_agg, campaign_ids, campaign_channels);
        if (!st2.ok()) {
            throw std::runtime_error("Failed writing campaign_daily parquet: " + st2.ToString());
        }

        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "Error: " << ex.what() << "\n";
        return 1;
    }
}
