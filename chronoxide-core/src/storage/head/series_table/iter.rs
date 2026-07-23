use std::collections::hash_map;

use crate::labels::SeriesRef;

use super::{AdaptiveSeriesTable, HeadSeriesTable, PAGE_LEN, SeriesPage};

pub(in crate::storage::head) enum Values<'a, V> {
    Plain(hash_map::Values<'a, SeriesRef, V>),
    Adaptive(AdaptiveValues<'a, V>),
}

impl<'a, V> Iterator for Values<'a, V> {
    type Item = &'a V;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Plain(values) => values.next(),
            Self::Adaptive(values) => values.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Plain(values) => values.size_hint(),
            Self::Adaptive(values) => values.size_hint(),
        }
    }
}

pub(in crate::storage::head) enum ValuesMut<'a, V> {
    Plain(hash_map::ValuesMut<'a, SeriesRef, V>),
    Adaptive(AdaptiveValuesMut<'a, V>),
}

impl<'a, V> Iterator for ValuesMut<'a, V> {
    type Item = &'a mut V;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Plain(values) => values.next(),
            Self::Adaptive(values) => values.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Plain(values) => values.size_hint(),
            Self::Adaptive(values) => values.size_hint(),
        }
    }
}

pub(in crate::storage::head) enum Keys<'a, V> {
    Plain(hash_map::Keys<'a, SeriesRef, V>),
    Adaptive(AdaptiveIter<'a, V>),
}

impl<V> Iterator for Keys<'_, V> {
    type Item = SeriesRef;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Plain(values) => values.next().copied(),
            Self::Adaptive(values) => values.next().map(|(series, _)| series),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Plain(values) => values.size_hint(),
            Self::Adaptive(values) => values.size_hint(),
        }
    }
}

pub(in crate::storage::head) enum Iter<'a, V> {
    Plain(hash_map::Iter<'a, SeriesRef, V>),
    Adaptive(AdaptiveIter<'a, V>),
}

impl<'a, V> Iterator for Iter<'a, V> {
    type Item = (SeriesRef, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Plain(values) => values.next().map(|(series, value)| (*series, value)),
            Self::Adaptive(values) => values.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Plain(values) => values.size_hint(),
            Self::Adaptive(values) => values.size_hint(),
        }
    }
}

pub(in crate::storage::head) enum IntoIter<V> {
    Plain(hash_map::IntoIter<SeriesRef, V>),
    Adaptive(AdaptiveIntoIter<V>),
}

impl<V> Iterator for IntoIter<V> {
    type Item = (SeriesRef, V);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Plain(values) => values.next(),
            Self::Adaptive(values) => values.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Plain(values) => values.size_hint(),
            Self::Adaptive(values) => values.size_hint(),
        }
    }
}

pub(in crate::storage::head) struct AdaptiveValues<'a, V> {
    sparse: hash_map::Values<'a, SeriesRef, V>,
    pages: std::slice::Iter<'a, SeriesPage<V>>,
    direct: Option<std::slice::Iter<'a, V>>,
    remaining: usize,
}

impl<'a, V> AdaptiveValues<'a, V> {
    pub(super) fn new(table: &'a AdaptiveSeriesTable<V>) -> Self {
        Self {
            sparse: table.sparse.values(),
            pages: table.pages.iter(),
            direct: None,
            remaining: table.len,
        }
    }
}

impl<'a, V> Iterator for AdaptiveValues<'a, V> {
    type Item = &'a V;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(value) = self.sparse.next() {
            self.remaining -= 1;
            return Some(value);
        }

        loop {
            if let Some(value) = self.direct.as_mut().and_then(Iterator::next) {
                self.remaining -= 1;
                return Some(value);
            }
            self.direct = None;
            let page = self.pages.next()?;
            if let SeriesPage::Direct(values) = page {
                self.direct = Some(values.values.iter());
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

pub(in crate::storage::head) struct AdaptiveValuesMut<'a, V> {
    sparse: hash_map::ValuesMut<'a, SeriesRef, V>,
    pages: std::slice::IterMut<'a, SeriesPage<V>>,
    direct: Option<std::slice::IterMut<'a, V>>,
    remaining: usize,
}

impl<'a, V> AdaptiveValuesMut<'a, V> {
    pub(super) fn new(table: &'a mut AdaptiveSeriesTable<V>) -> Self {
        Self {
            sparse: table.sparse.values_mut(),
            pages: table.pages.iter_mut(),
            direct: None,
            remaining: table.len,
        }
    }
}

impl<'a, V> Iterator for AdaptiveValuesMut<'a, V> {
    type Item = &'a mut V;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(value) = self.sparse.next() {
            self.remaining -= 1;
            return Some(value);
        }

        loop {
            if let Some(value) = self.direct.as_mut().and_then(Iterator::next) {
                self.remaining -= 1;
                return Some(value);
            }
            self.direct = None;
            let page = self.pages.next()?;
            if let SeriesPage::Direct(values) = page {
                self.direct = Some(values.values.iter_mut());
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

pub(in crate::storage::head) struct AdaptiveIter<'a, V> {
    sparse: hash_map::Iter<'a, SeriesRef, V>,
    pages: std::iter::Enumerate<std::slice::Iter<'a, SeriesPage<V>>>,
    direct: Option<(u32, DirectIter<'a, V>)>,
    remaining: usize,
}

type DirectIter<'a, V> = std::iter::Zip<std::slice::Iter<'a, u16>, std::slice::Iter<'a, V>>;

impl<'a, V> AdaptiveIter<'a, V> {
    pub(super) fn new(table: &'a AdaptiveSeriesTable<V>) -> Self {
        Self {
            sparse: table.sparse.iter(),
            pages: table.pages.iter().enumerate(),
            direct: None,
            remaining: table.len,
        }
    }
}

impl<'a, V> Iterator for AdaptiveIter<'a, V> {
    type Item = (SeriesRef, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((series, value)) = self.sparse.next() {
            self.remaining -= 1;
            return Some((*series, value));
        }

        loop {
            if let Some((first_ref, entries)) = &mut self.direct
                && let Some((slot, value)) = entries.next()
            {
                self.remaining -= 1;
                return Some((SeriesRef::new(*first_ref + u32::from(*slot)), value));
            }
            self.direct = None;
            let (page_index, page) = self.pages.next()?;
            if let SeriesPage::Direct(values) = page {
                self.direct = Some((
                    (page_index * PAGE_LEN) as u32,
                    values.reverse_slots.iter().zip(values.values.iter()),
                ));
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

pub(in crate::storage::head) struct AdaptiveIntoIter<V> {
    sparse: hash_map::IntoIter<SeriesRef, V>,
    pages: std::iter::Enumerate<std::vec::IntoIter<SeriesPage<V>>>,
    direct: Option<(u32, DirectIntoIter<V>)>,
    remaining: usize,
}

type DirectIntoIter<V> = std::iter::Zip<std::vec::IntoIter<u16>, std::vec::IntoIter<V>>;

impl<V> AdaptiveIntoIter<V> {
    pub(super) fn new(table: AdaptiveSeriesTable<V>) -> Self {
        Self {
            sparse: table.sparse.into_iter(),
            pages: table.pages.into_iter().enumerate(),
            direct: None,
            remaining: table.len,
        }
    }
}

impl<V> Iterator for AdaptiveIntoIter<V> {
    type Item = (SeriesRef, V);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(entry) = self.sparse.next() {
            self.remaining -= 1;
            return Some(entry);
        }

        loop {
            if let Some((first_ref, entries)) = &mut self.direct
                && let Some((slot, value)) = entries.next()
            {
                self.remaining -= 1;
                return Some((SeriesRef::new(*first_ref + u32::from(slot)), value));
            }
            self.direct = None;
            let (page_index, page) = self.pages.next()?;
            if let SeriesPage::Direct(values) = page {
                self.direct = Some((
                    (page_index * PAGE_LEN) as u32,
                    values.reverse_slots.into_iter().zip(values.values),
                ));
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<V> IntoIterator for HeadSeriesTable<V> {
    type Item = (SeriesRef, V);
    type IntoIter = IntoIter<V>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Self::Plain { values, .. } => IntoIter::Plain(values.into_iter()),
            Self::Adaptive(values) => IntoIter::Adaptive(AdaptiveIntoIter::new(values)),
        }
    }
}
