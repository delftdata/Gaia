#pragma once

#include <string>
#include <array>
#include <charconv>
#include "execution/dsh/scalar.h"

namespace slog {
namespace dsh {

const uint32_t kMaxStay = 14u;
const double kMaxSearchRadius = 0.1; 
const uint16_t kRecommendationReadSize = 10u;

const double kMaxHotelPrice = 10000.0;
const uint32_t kMinHotelCapacity = 10, kMaxHotelCapacity = 500;

std::vector<FixedTextScalarPtr> date_interp(std::string in_date, std::string out_date);
std::string format_date(int d, int m, int y);
std::string format_uname(std::string uname);
inline double dist(double x1, double y1, double x2, double y2) {
    return (x1 - x2) * (x1 - x2) + (y1 - y2) * (y1 - y2); 
};

}  // namespace tpcc
}  // namespace slog
