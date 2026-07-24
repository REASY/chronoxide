/// Intersects two sorted, duplicate-free slices.
///
/// The result preserves ascending order. The intentionally unreserved output
/// matches the former head and sealed-segment implementations.
#[inline]
pub(in crate::storage) fn intersect_sorted<T: Copy + Ord>(left: &[T], right: &[T]) -> Vec<T> {
    let mut out = Vec::new();
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    while left_index < left.len() && right_index < right.len() {
        match left[left_index].cmp(&right[right_index]) {
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
            std::cmp::Ordering::Equal => {
                out.push(left[left_index]);
                left_index += 1;
                right_index += 1;
            }
        }
    }
    out
}

/// Unions two sorted, duplicate-free slices.
///
/// The result preserves ascending order and uniqueness. Reserving the sum of
/// both input lengths matches the former head and sealed-segment
/// implementations.
#[inline]
pub(in crate::storage) fn union_sorted<T: Copy + Ord>(left: &[T], right: &[T]) -> Vec<T> {
    let mut out = Vec::with_capacity(left.len().saturating_add(right.len()));
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    while left_index < left.len() || right_index < right.len() {
        if left_index >= left.len() {
            out.extend_from_slice(&right[right_index..]);
            break;
        }
        if right_index >= right.len() {
            out.extend_from_slice(&left[left_index..]);
            break;
        }

        match left[left_index].cmp(&right[right_index]) {
            std::cmp::Ordering::Less => {
                out.push(left[left_index]);
                left_index += 1;
            }
            std::cmp::Ordering::Greater => {
                out.push(right[right_index]);
                right_index += 1;
            }
            std::cmp::Ordering::Equal => {
                out.push(left[left_index]);
                left_index += 1;
                right_index += 1;
            }
        }
    }
    out
}

/// Subtracts the sorted, duplicate-free `right` slice from `left`.
///
/// The result preserves ascending order. The intentionally unreserved output
/// matches the former head and sealed-segment implementations.
#[inline]
pub(in crate::storage) fn subtract_sorted<T: Copy + Ord>(left: &[T], right: &[T]) -> Vec<T> {
    let mut out = Vec::new();
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    while left_index < left.len() {
        if right_index >= right.len() {
            out.extend_from_slice(&left[left_index..]);
            break;
        }

        match left[left_index].cmp(&right[right_index]) {
            std::cmp::Ordering::Less => {
                out.push(left[left_index]);
                left_index += 1;
            }
            std::cmp::Ordering::Greater => right_index += 1,
            std::cmp::Ordering::Equal => {
                left_index += 1;
                right_index += 1;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const LEFT: &[u32] = &[1, 3, 5, 8];
    const RIGHT: &[u32] = &[0, 3, 4, 8, 9];

    #[test]
    fn intersect_sorted_preserves_common_values_in_order() {
        assert_eq!(intersect_sorted(LEFT, RIGHT), vec![3, 8]);
        assert_eq!(intersect_sorted(LEFT, &[]), Vec::<u32>::new());
        assert_eq!(intersect_sorted(&[], RIGHT), Vec::<u32>::new());
    }

    #[test]
    fn union_sorted_preserves_all_unique_values_in_order() {
        assert_eq!(union_sorted(LEFT, RIGHT), vec![0, 1, 3, 4, 5, 8, 9]);
        assert_eq!(union_sorted(LEFT, &[]), LEFT);
        assert_eq!(union_sorted(&[], RIGHT), RIGHT);
    }

    #[test]
    fn subtract_sorted_preserves_only_left_exclusive_values_in_order() {
        assert_eq!(subtract_sorted(LEFT, RIGHT), vec![1, 5]);
        assert_eq!(subtract_sorted(LEFT, &[]), LEFT);
        assert_eq!(subtract_sorted(&[], RIGHT), Vec::<u32>::new());
    }
}
