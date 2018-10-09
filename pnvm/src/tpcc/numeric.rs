//***************************************
// Numeric types for decimal and integer
// types with length and precision requirement
//***************************************
use std::cmp::{Ordering,max};
use num::pow::pow;
use std::ops::{Sub, Mul, Add, SubAssign,AddAssign};

#[derive(PartialOrd, Clone, Debug, Eq, Copy)]
pub struct Numeric {
    value: i64,
    len: usize,
    precision: usize
}


impl Numeric {
    pub fn new(value: i64, len: usize, precision: usize) -> Numeric {
        Numeric {
            value: value,
            len: len,
            precision: precision
        }
    }
    pub fn from_str(s: &str, len: usize, precision: usize) -> Option<Numeric> {
        let mut s = s.trim();
        let mut value : i64  = 0;
        let mut negative = false;
        let mut fraction = false;
        if &s[0..1] == "-" {
            negative = true;
            s = &s[1..];
        }
        if s.find('.').is_some() {
            while s.chars().last() == Some('0') {
                s = &s[..s.len() - 1]
            }
        }
        let mut digits_seen = 0;
        let mut digits_seen_fraction = 0;
        for c in s.chars() {
            if let Some(n) = c.to_digit(10) {
                value = value * 10 + n as i64;
                if fraction {
                    digits_seen_fraction += 1;
                } else {
                    digits_seen += 1;
                }
            } else if c == '.' {
                fraction = match fraction {
                    true => return None,
                    false => true
                };
            } else {
                return None;
            }
        }
        if negative {
            value *= -1;
        }
        if digits_seen > len - precision || digits_seen_fraction > precision {
            None
        } else {
            Some(Numeric::new(value * pow(10, precision - digits_seen_fraction), len, precision))
        }
    }

    pub fn as_string(&self) -> String {
        format!("{}", self.value / pow(10, self.precision))
    }
}
impl PartialEq for Numeric {
    fn eq(&self, other: &Numeric) -> bool {
        self.value == other.value
            && self.precision == other.precision
    }
}
impl Ord for Numeric {
    fn cmp(&self, other: &Numeric) -> Ordering {
        match self.precision.cmp(&other.precision) {
            Ordering::Equal => self.value.cmp(&other.value),
            Ordering::Less => (self.value * pow(10, other.precision - self.precision)).cmp(&other.value),
            Ordering::Greater => (other.value * pow(10, self.precision - other.precision)).cmp(&self.value),
        }
    }
}
impl Add for Numeric {
    type Output = Numeric;
    fn add(self, rhs: Numeric) -> Numeric {
        Numeric {
            value: match self.precision.cmp(&rhs.precision) {
                Ordering::Equal => self.value + rhs.value,
                Ordering::Less => self.value * pow(10, rhs.precision - self.precision) + rhs.value,
                Ordering::Greater => rhs.value * pow(10, self.precision - rhs.precision) + self.value,
            },
            precision: max(self.precision, rhs.precision),
            len: max(self.len, rhs.len)
        }
    }
}

impl AddAssign for Numeric {
    fn add_assign(&mut self, rhs : Numeric) {
        *self =  Numeric {
            value: match self.precision.cmp(&rhs.precision) {
                Ordering::Equal => self.value + rhs.value,
                Ordering::Less => self.value * pow(10, rhs.precision - self.precision) + rhs.value,
                Ordering::Greater => rhs.value * pow(10, self.precision - rhs.precision) + self.value,
            },
            precision: max(self.precision, rhs.precision),
            len: max(self.len, rhs.len)
        };
    }

}

impl Sub for Numeric {
    type Output = Numeric;
    fn sub(self, rhs: Numeric) -> Numeric {
        Numeric {
            value: match self.precision.cmp(&rhs.precision) {
                Ordering::Equal => self.value - rhs.value,
                Ordering::Less => self.value * pow(10, rhs.precision - self.precision) - rhs.value,
                Ordering::Greater => self.value - rhs.value * pow(10, self.precision - rhs.precision),
            },
            precision: max(self.precision, rhs.precision),
            len: max(self.len, rhs.len)
        }
    }
}


impl SubAssign for Numeric {
    
    fn sub_assign(&mut self, rhs : Numeric) {
        *self = Numeric {
            value: match self.precision.cmp(&rhs.precision) {
                Ordering::Equal => self.value - rhs.value,
                Ordering::Less => self.value * pow(10, rhs.precision - self.precision) - rhs.value,
                Ordering::Greater => self.value - rhs.value * pow(10, self.precision - rhs.precision),
            },
            precision: max(self.precision, rhs.precision),
            len: max(self.len, rhs.len)
        }
    }

}


impl Mul for Numeric {
    type Output = Numeric;
    fn mul(self, rhs: Numeric) -> Numeric {
        Numeric {
            value: match self.precision.cmp(&rhs.precision) {
                Ordering::Equal => self.value * rhs.value,
                Ordering::Less => self.value * pow(10, rhs.precision - self.precision) * rhs.value,
                Ordering::Greater => self.value * rhs.value * pow(10, self.precision * rhs.precision),
            },
            precision: max(self.precision, rhs.precision),
            len: max(self.len, rhs.len)
        }
    }
}


