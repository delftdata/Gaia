#include "execution/dsh/utils.h"

namespace slog {
namespace dsh {

std::string format_date(int d, int m, int y) {
    return std::string(d < 10, '0') + std::to_string(d) + "-" + std::string(m < 10, '0') + std::to_string(m) + "-" + std::to_string(y);
}

std::string format_uname(std::string uname) {
    size_t l = uname.length();
    if (l > 18) {
        LOG(FATAL) << "invalid username, must be <=18 characters long" << uname;
    }
    // create username with proper format
    return "0" + std::to_string(l) + std::string(18 - l, '_') + uname;
}

// assumes nice date format
std::vector<FixedTextScalarPtr> date_interp(std::string in_date, std::string out_date) {
    int d1, m1, y1;
    int d2, m2, y2;
    std::from_chars(in_date.data(), in_date.data() + 2, d1);
    std::from_chars(in_date.data() + 3, in_date.data() + 5, m1);
    std::from_chars(in_date.data() + 6, in_date.data() + 10, y1);
    std::from_chars(out_date.data(), out_date.data() + 2, d2);
    std::from_chars(out_date.data() + 3, out_date.data() + 5, m2);
    std::from_chars(out_date.data() + 6, out_date.data() + 10, y2);
    // I know i should fix this -- guard clause for out_date later than in_date
    if (!(y1 < y2 || (y1 == y2 && (m1 < m2 || (m1 == m2 && d1 < d2))))) {
        return {};
    }
    std::vector<FixedTextScalarPtr> rval;
    // Leap years are fake and I don't want to deal with them
    const int months_in_year[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    while (d1 != d2 || m1 != m2 || y1 != y2) {
        if (d1 > 31) {
            LOG(INFO) << in_date << " --- " << out_date;
        }
        rval.push_back(MakeFixedTextScalar<10>(format_date(d1, m1, y1)));
        d1++;
        if (d1 > months_in_year[m1 - 1]) {
            d1 = 1;
            m1++;
            if (m1 > 12) {
                m1 = 1;
                y1++;
            }
        }
    }
    // don't include the end of the stay in the reservation table
    return rval;
}


} // namespace dsh
} // namespace slog

