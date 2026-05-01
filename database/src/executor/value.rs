use anyhow::{Result, anyhow};
use common::{Data, query::ComparisionOperator};
use std::cmp::Ordering;

use crate::row::Row;

use super::CompiledSortSpec;

pub fn compare_rows(left: &Row, right: &Row, specs: &[CompiledSortSpec]) -> Ordering {
    for spec in specs {
        let left_value = left
            .get(spec.index)
            .expect("compiled sort index should exist in left row");
        let right_value = right
            .get(spec.index)
            .expect("compiled sort index should exist in right row");
        let ordering = compare_values(left_value, right_value).unwrap_or(Ordering::Equal);
        let ordering = if spec.ascending {
            ordering
        } else {
            ordering.reverse()
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }

    Ordering::Equal
}

pub fn compare_values(left: &Data, right: &Data) -> Result<Ordering> {
    compare_data_order(left, right).ok_or_else(|| anyhow!("incomparable types"))
}

pub fn compare_data(left: &Data, op: &ComparisionOperator, right: &Data) -> Result<bool> {
    let ordering = compare_values(left, right)?;
    Ok(match op {
        ComparisionOperator::EQ => ordering == Ordering::Equal,
        ComparisionOperator::NE => ordering != Ordering::Equal,
        ComparisionOperator::GT => ordering == Ordering::Greater,
        ComparisionOperator::GTE => ordering != Ordering::Less,
        ComparisionOperator::LT => ordering == Ordering::Less,
        ComparisionOperator::LTE => ordering != Ordering::Greater,
    })
}

pub(crate) fn exact_integral_i64_from_f64(value: f64) -> Option<i64> {
    if !value.is_finite() {
        return None;
    }

    if value == 0.0 {
        return Some(0);
    }

    let is_negative = value.is_sign_negative();
    let binary = BinaryFloat::from_f64(value.abs())?;

    if binary.exponent >= 0 {
        let shift = binary.exponent as u32;
        let integer = (binary.significand as u128).checked_shl(shift)?;
        return signed_i64_from_u128(integer, is_negative);
    }

    let shift = (-binary.exponent) as u32;
    if shift >= u64::BITS {
        return None;
    }

    let fractional_mask = (1u64 << shift) - 1;
    if (binary.significand & fractional_mask) != 0 {
        return None;
    }

    signed_i64_from_u128((binary.significand >> shift) as u128, is_negative)
}

pub(crate) fn normalized_f64_bits(value: f64) -> u64 {
    if value == 0.0 {
        0.0f64.to_bits()
    } else {
        value.to_bits()
    }
}

fn compare_data_order(left: &Data, right: &Data) -> Option<Ordering> {
    left.partial_cmp(right)
        .or_else(|| compare_numeric_order(left, right))
}

fn compare_numeric_order(left: &Data, right: &Data) -> Option<Ordering> {
    match (numeric_kind(left), numeric_kind(right)) {
        (Some(NumericKind::Int(left_value)), Some(NumericKind::Int(right_value))) => {
            Some(left_value.cmp(&right_value))
        }
        (Some(NumericKind::Int(left_value)), Some(NumericKind::Float(right_value))) => {
            compare_int_to_float(left_value, right_value)
        }
        (Some(NumericKind::Float(left_value)), Some(NumericKind::Int(right_value))) => {
            compare_int_to_float(right_value, left_value).map(Ordering::reverse)
        }
        (Some(NumericKind::Float(left_value)), Some(NumericKind::Float(right_value))) => {
            left_value.partial_cmp(&right_value)
        }
        _ => None,
    }
}

#[derive(Copy, Clone)]
enum NumericKind {
    Int(i64),
    Float(f64),
}

fn numeric_kind(value: &Data) -> Option<NumericKind> {
    match value {
        Data::Int32(v) => Some(NumericKind::Int(*v as i64)),
        Data::Int64(v) => Some(NumericKind::Int(*v)),
        Data::Float32(v) => Some(NumericKind::Float(*v as f64)),
        Data::Float64(v) => Some(NumericKind::Float(*v)),
        Data::String(_) => None,
    }
}

fn compare_int_to_float(int_value: i64, float_value: f64) -> Option<Ordering> {
    if float_value.is_nan() {
        return None;
    }

    if float_value.is_infinite() {
        return Some(if float_value.is_sign_positive() {
            Ordering::Less
        } else {
            Ordering::Greater
        });
    }

    if float_value == 0.0 {
        return Some(int_value.cmp(&0));
    }

    let int_is_negative = int_value.is_negative();
    let float_is_negative = float_value.is_sign_negative();
    if int_is_negative != float_is_negative {
        return Some(if int_is_negative {
            Ordering::Less
        } else {
            Ordering::Greater
        });
    }

    let magnitude_ordering =
        compare_positive_int_to_positive_float(int_value.unsigned_abs(), float_value.abs())?;
    Some(if int_is_negative {
        magnitude_ordering.reverse()
    } else {
        magnitude_ordering
    })
}

fn signed_i64_from_u128(magnitude: u128, is_negative: bool) -> Option<i64> {
    if is_negative {
        let min_magnitude = (i64::MAX as u128) + 1;
        if magnitude > min_magnitude {
            None
        } else if magnitude == min_magnitude {
            Some(i64::MIN)
        } else {
            Some(-(magnitude as i64))
        }
    } else if magnitude > i64::MAX as u128 {
        None
    } else {
        Some(magnitude as i64)
    }
}

fn compare_positive_int_to_positive_float(int_value: u64, float_value: f64) -> Option<Ordering> {
    debug_assert!(float_value.is_finite());
    debug_assert!(float_value.is_sign_positive());

    let binary = BinaryFloat::from_f64(float_value)?;

    if binary.exponent >= 0 {
        let shift = binary.exponent as u32;
        let float_bit_length = bit_length_u64(binary.significand).saturating_add(shift);
        let int_bit_length = bit_length_u64(int_value);
        if int_bit_length != float_bit_length {
            return Some(int_bit_length.cmp(&float_bit_length));
        }

        let float_integer = (binary.significand as u128) << shift;
        return Some((int_value as u128).cmp(&float_integer));
    }

    let shift = (-binary.exponent) as u32;
    let integer_part = if shift >= u64::BITS {
        0
    } else {
        binary.significand >> shift
    };
    let has_fraction = if shift == 0 {
        false
    } else if shift >= u64::BITS {
        binary.significand != 0
    } else {
        let fractional_mask = (1u64 << shift) - 1;
        (binary.significand & fractional_mask) != 0
    };

    Some(match int_value.cmp(&integer_part) {
        Ordering::Equal if has_fraction => Ordering::Less,
        other => other,
    })
}

fn bit_length_u64(value: u64) -> u32 {
    u64::BITS - value.leading_zeros()
}

#[derive(Copy, Clone)]
struct BinaryFloat {
    significand: u64,
    exponent: i32,
}

impl BinaryFloat {
    fn from_f64(value: f64) -> Option<Self> {
        if !value.is_finite() || value == 0.0 {
            return None;
        }

        let bits = value.to_bits();
        let exponent_bits = ((bits >> 52) & 0x7ff) as i32;
        let mantissa_bits = bits & ((1u64 << 52) - 1);

        Some(if exponent_bits == 0 {
            Self {
                significand: mantissa_bits,
                exponent: -1074,
            }
        } else {
            Self {
                significand: (1u64 << 52) | mantissa_bits,
                exponent: exponent_bits - 1075,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compares_large_int_and_float_without_f64_rounding_loss() -> Result<()> {
        assert!(compare_data(
            &Data::Int64(9_007_199_254_740_993),
            &ComparisionOperator::GT,
            &Data::Float64(9_007_199_254_740_992.0),
        )?);
        Ok(())
    }

    #[test]
    fn compares_integral_float_and_int_as_equal() -> Result<()> {
        assert!(compare_data(
            &Data::Int64(42),
            &ComparisionOperator::EQ,
            &Data::Float64(42.0),
        )?);
        Ok(())
    }

    #[test]
    fn compare_rows_uses_same_numeric_policy_as_predicates() {
        let left = Row::new(vec![Data::Int64(42)]);
        let right = Row::new(vec![Data::Float64(42.5)]);
        let specs = vec![CompiledSortSpec {
            index: 0,
            ascending: true,
        }];

        assert_eq!(compare_rows(&left, &right, &specs), Ordering::Less);
    }
}
