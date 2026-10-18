// Chính sách giảm giá theo điểm thành viên (score) - trả về phần trăm giảm giá
//   score >= 100 -> 15%   (Gold)
//   score >=  50 -> 10%   (Silver)
//   score >=  10 ->  5%   (Normal, hạng mặc định khi đăng ký)
//   score <   10 ->  0%
function discountForScore(score) {
  if (score >= 100) return 15;
  if (score >= 50) return 10;
  if (score >= 10) return 5;
  return 0;
}

const round2 = (n) => Math.round((n + Number.EPSILON) * 100) / 100;

function calcTotals(lines, discountPercent) {
  const subtotal = round2(lines.reduce((s, l) => s + l.qty * l.unit_price, 0));
  const total = round2(subtotal * (1 - discountPercent / 100));
  return { subtotal, total };
}

module.exports = { discountForScore, calcTotals, round2 };
