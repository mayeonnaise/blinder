use std::{
    mem,
    ops::{Bound, Index, IndexMut, RangeBounds},
    slice::{self, SliceIndex},
};

pub struct List<'a, T> {
    items: &'a mut Vec<T>,
    offset: usize,
}

impl<'a, T> List<'a, T> {
    pub fn new(items: &'a mut Vec<T>) -> Self {
        Self { items, offset: 0 }
    }

    pub fn len(&self) -> usize {
        self.items.len() - self.offset
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn push(&mut self, item: T) {
        self.items.push(item)
    }

    pub fn drain(&mut self, range: impl RangeBounds<usize>) -> impl Iterator<Item = T> + '_ {
        self.items.drain((
            match range.start_bound() {
                Bound::Included(&start) => Bound::Included(self.offset + start),
                Bound::Excluded(&start) => Bound::Excluded(self.offset + start),
                Bound::Unbounded => Bound::Included(self.offset),
            },
            match range.end_bound() {
                Bound::Included(&end) => Bound::Included(self.offset + end),
                Bound::Excluded(&end) => Bound::Excluded(self.offset + end),
                Bound::Unbounded => Bound::Unbounded,
            },
        ))
    }

    pub fn map_in_place(&mut self, mut f: impl FnMut(T) -> T) {
        *self.items = mem::take(self.items)
            .into_iter()
            .enumerate()
            .map(|(i, item)| if i < self.offset { item } else { f(item) })
            .collect()
    }

    pub fn saved(&'_ mut self) -> List<'_, T> {
        let offset = self.items.len();
        List {
            items: self.items,
            offset,
        }
    }
}

impl<T, I: SliceIndex<[T]>> Index<I> for List<'_, T> {
    type Output = I::Output;

    fn index(&self, index: I) -> &Self::Output {
        self.items[self.offset..].index(index)
    }
}

impl<T, I: SliceIndex<[T]>> IndexMut<I> for List<'_, T> {
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        self.items[self.offset..].index_mut(index)
    }
}

impl<'a, T> IntoIterator for &'a List<'_, T> {
    type Item = &'a T;
    type IntoIter = slice::Iter<'a, T>;

    fn into_iter(self) -> slice::Iter<'a, T> {
        self.items[self.offset..].iter()
    }
}

impl<'a, T> IntoIterator for &'a mut List<'_, T> {
    type Item = &'a mut T;
    type IntoIter = slice::IterMut<'a, T>;

    fn into_iter(self) -> slice::IterMut<'a, T> {
        self.items[self.offset..].iter_mut()
    }
}

impl<T> Extend<T> for List<'_, T> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        self.items.extend(iter);
    }
}
