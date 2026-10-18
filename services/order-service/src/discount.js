// Domain rule: chinh sach giam gia theo score thanh vien (FR08 / UC07).
//   score >= 100 -> 20%   (Gold)
//   score >=  50 -> 15%   (Silver)
//   score >=  10 -> 10%   (Normal)
//   con lai      ->  0%
function calculateDiscount(score) {
  if (score >= 100) return 20;
  if (score >= 50) return 15;
  if (score >= 10) return 10;
  return 0;
}

module.exports = { calculateDiscount };
