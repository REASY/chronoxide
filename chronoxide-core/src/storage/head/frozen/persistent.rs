//! Persistent live-head sample descriptors.
//!
//! Encoded payloads remain owned by [`FrozenHeadFragment`].  This module only
//! path-copies immutable map nodes and builds constant-size concat nodes over
//! fragment/run references.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt;
use std::io;
use std::sync::Arc;

use crate::labels::SeriesRef;
use crate::storage::live_coverage::{
    MessageSequence, RecordedSampleOrder, RecordedSampleOrderRange,
};

use super::{FrozenHeadFragment, FrozenHeadLane, FrozenSeriesRun, SampleKind};

const MAX_DESCRIPTOR_DEPTH: u8 = u64::BITS as u8;

/// A source partition qualified by its topic.
///
/// Kafka partition numbers are only unique within a topic.  Keeping the topic
/// in the ordered identity prevents two equal numeric partition IDs from
/// aliasing in a live view.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LivePartitionKey {
    topic: Arc<str>,
    partition: i32,
}

impl fmt::Debug for LivePartitionKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LivePartitionKey")
            .field("topic", &self.topic)
            .field("partition", &self.partition)
            .finish()
    }
}

impl LivePartitionKey {
    pub fn new(topic: impl Into<Arc<str>>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub const fn partition(&self) -> i32 {
        self.partition
    }

    fn compatibility(partition: i32) -> Self {
        Self::new("<legacy-frozen-head>", partition)
    }
}

/// Full immutable fragment identity used by the persistent sample map.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct FrozenFragmentKey {
    partition: LivePartitionKey,
    start_ms: u64,
    end_ms: u64,
    lane: FrozenHeadLane,
}

impl FrozenFragmentKey {
    pub fn new(
        partition: LivePartitionKey,
        start_ms: u64,
        end_ms: u64,
        lane: FrozenHeadLane,
    ) -> io::Result<Self> {
        if end_ms <= start_ms {
            return Err(invalid_data("frozen fragment range must be non-empty"));
        }
        Ok(Self {
            partition,
            start_ms,
            end_ms,
            lane,
        })
    }

    pub fn partition_key(&self) -> &LivePartitionKey {
        &self.partition
    }

    pub const fn start_ms(&self) -> u64 {
        self.start_ms
    }

    pub const fn end_ms(&self) -> u64 {
        self.end_ms
    }

    pub const fn lane(&self) -> FrozenHeadLane {
        self.lane
    }

    fn overlaps(&self, start_ms: u64, end_ms: u64) -> bool {
        self.end_ms > start_ms && self.start_ms <= end_ms
    }
}

/// Full persistent-map key for one encoded series/kind run.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct LiveSampleKey {
    fragment: FrozenFragmentKey,
    series: SeriesRef,
    kind: SampleKind,
}

impl LiveSampleKey {
    pub fn new(fragment: FrozenFragmentKey, series: SeriesRef, kind: SampleKind) -> Self {
        Self {
            fragment,
            series,
            kind,
        }
    }

    pub fn fragment_key(&self) -> &FrozenFragmentKey {
        &self.fragment
    }

    pub const fn series(&self) -> SeriesRef {
        self.series
    }

    pub const fn kind(&self) -> SampleKind {
        self.kind
    }
}

/// Identity and stable ingest-order range attached to one frozen fragment.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct FrozenFragmentIdentity {
    key: FrozenFragmentKey,
    order_range: RecordedSampleOrderRange,
}

impl FrozenFragmentIdentity {
    pub fn new(
        key: FrozenFragmentKey,
        first: RecordedSampleOrder,
        last: RecordedSampleOrder,
    ) -> io::Result<Self> {
        let order_range = checked_order_range(first, last)?;
        Ok(Self { key, order_range })
    }

    /// Derives the exact range/lane and recorded order from a frozen fragment.
    ///
    /// Tracked live fragments carry their real message/sample range.  The
    /// compatibility frozen-head path predates that ledger and falls back to
    /// its strictly increasing publication sequence.
    pub fn for_fragment(
        partition: LivePartitionKey,
        fragment: &FrozenHeadFragment,
    ) -> io::Result<Self> {
        let key = FrozenFragmentKey::new(
            partition,
            fragment.start_ms(),
            fragment.end_ms(),
            fragment.lane(),
        )?;
        let order_range = match fragment.recorded_order_range() {
            Some(range) => range,
            None => {
                let sequence = fragment.publication_sequence();
                if sequence == 0 {
                    return Err(invalid_data(
                        "untracked frozen fragment has no publication sequence",
                    ));
                }
                RecordedSampleOrderRange::one(RecordedSampleOrder::new(
                    MessageSequence::new(sequence),
                    0,
                ))
            }
        };
        Ok(Self { key, order_range })
    }

    pub fn fragment_key(&self) -> &FrozenFragmentKey {
        &self.key
    }

    pub const fn order_range(&self) -> RecordedSampleOrderRange {
        self.order_range
    }

    fn compatibility(fragment: &FrozenHeadFragment, sorted_index: usize) -> io::Result<Self> {
        let partition = i32::try_from(sorted_index)
            .map_err(|_| invalid_data("too many compatibility frozen fragments"))?;
        let sequence = u64::try_from(sorted_index)
            .ok()
            .and_then(|index| index.checked_add(1))
            .ok_or_else(|| invalid_data("compatibility fragment sequence overflows u64"))?;
        let key = FrozenFragmentKey::new(
            LivePartitionKey::compatibility(partition),
            fragment.start_ms(),
            fragment.end_ms(),
            fragment.lane(),
        )?;
        let order = RecordedSampleOrder::new(MessageSequence::new(sequence), 0);
        Self::new(key, order, order)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DescriptorIdentity {
    key: LiveSampleKey,
    codec: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DescriptorMeta {
    identity: Arc<DescriptorIdentity>,
    first: RecordedSampleOrder,
    last: RecordedSampleOrder,
    samples: u64,
    blocks: u64,
    leaves: u64,
    nodes: u64,
    depth: u8,
}

#[derive(Debug)]
struct DescriptorLeaf {
    meta: DescriptorMeta,
    fragment: Arc<FrozenHeadFragment>,
}

#[derive(Debug)]
struct DescriptorConcat {
    meta: DescriptorMeta,
    older: Arc<DescriptorNode>,
    newer: Arc<DescriptorNode>,
}

#[derive(Debug)]
enum DescriptorNode {
    Leaf(DescriptorLeaf),
    Concat(DescriptorConcat),
}

impl DescriptorNode {
    fn meta(&self) -> &DescriptorMeta {
        match self {
            Self::Leaf(leaf) => &leaf.meta,
            Self::Concat(concat) => &concat.meta,
        }
    }

    fn leaf(
        identity: Arc<DescriptorIdentity>,
        fragment: Arc<FrozenHeadFragment>,
        run: &FrozenSeriesRun,
        order_range: RecordedSampleOrderRange,
    ) -> io::Result<Arc<Self>> {
        if run.series != identity.key.series || run.kind != identity.key.kind {
            return Err(invalid_data(
                "frozen run does not match its persistent sample identity",
            ));
        }
        if run.encoded.codec_name() != identity.codec {
            return Err(invalid_data(
                "frozen run codec does not match its descriptor identity",
            ));
        }
        let blocks = u64::try_from(run.encoded.block_count())
            .map_err(|_| invalid_data("frozen run block count overflows u64"))?;
        Ok(Arc::new(Self::Leaf(DescriptorLeaf {
            meta: DescriptorMeta {
                identity,
                first: order_range.first(),
                last: order_range.last(),
                samples: run.encoded.sample_count(),
                blocks,
                leaves: 1,
                nodes: 1,
                depth: 1,
            },
            fragment,
        })))
    }

    fn concat(older: Arc<Self>, newer: Arc<Self>) -> io::Result<Arc<Self>> {
        let older_meta = older.meta();
        let newer_meta = newer.meta();
        if older_meta.identity.as_ref() != newer_meta.identity.as_ref() {
            return Err(invalid_data(
                "descriptor concat identities or codecs do not match",
            ));
        }
        if older_meta.last >= newer_meta.first {
            return Err(invalid_data(
                "descriptor sequence ranges overlap or are out of order",
            ));
        }
        let depth = older_meta
            .depth
            .max(newer_meta.depth)
            .checked_add(1)
            .ok_or_else(|| invalid_data("descriptor depth overflows u8"))?;
        if depth > MAX_DESCRIPTOR_DEPTH {
            return Err(invalid_data(
                "descriptor depth exceeds the u64 publication bound",
            ));
        }
        let samples = checked_sum(older_meta.samples, newer_meta.samples, "sample")?;
        let blocks = checked_sum(older_meta.blocks, newer_meta.blocks, "block")?;
        let leaves = checked_sum(older_meta.leaves, newer_meta.leaves, "leaf")?;
        let child_nodes = checked_sum(older_meta.nodes, newer_meta.nodes, "node")?;
        let nodes = child_nodes
            .checked_add(1)
            .ok_or_else(|| invalid_data("descriptor node count overflows u64"))?;
        Ok(Arc::new(Self::Concat(DescriptorConcat {
            meta: DescriptorMeta {
                identity: Arc::clone(&older_meta.identity),
                first: older_meta.first,
                last: newer_meta.last,
                samples,
                blocks,
                leaves,
                nodes,
                depth,
            },
            older,
            newer,
        })))
    }

    /// Iteratively validates and traverses older leaves before newer leaves.
    fn append_leaves(&self, output: &mut Vec<FrozenRunRef>) -> io::Result<()> {
        let root_meta = self.meta();
        if root_meta.depth == 0 || root_meta.depth > MAX_DESCRIPTOR_DEPTH {
            return Err(invalid_data("invalid descriptor root depth"));
        }
        let capacity = usize::from(root_meta.depth);
        let mut stack = Vec::new();
        stack
            .try_reserve_exact(capacity)
            .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
        stack.push(self);

        let mut previous_last = None;
        let mut samples = 0u64;
        let mut blocks = 0u64;
        let mut leaves = 0u64;
        let mut nodes = 0u64;
        while let Some(node) = stack.pop() {
            nodes = nodes
                .checked_add(1)
                .ok_or_else(|| invalid_data("traversed descriptor node count overflows u64"))?;
            match node {
                Self::Leaf(leaf) => {
                    validate_leaf(leaf)?;
                    if previous_last.is_some_and(|last| last >= leaf.meta.first) {
                        return Err(invalid_data(
                            "descriptor leaves are not in strict sequence order",
                        ));
                    }
                    previous_last = Some(leaf.meta.last);
                    samples = checked_sum(samples, leaf.meta.samples, "sample")?;
                    blocks = checked_sum(blocks, leaf.meta.blocks, "block")?;
                    leaves = checked_sum(leaves, 1, "leaf")?;
                    output.push(FrozenRunRef {
                        key: leaf.meta.identity.key.clone(),
                        first: leaf.meta.first,
                        last: leaf.meta.last,
                        fragment: Arc::clone(&leaf.fragment),
                    });
                }
                Self::Concat(concat) => {
                    validate_concat(concat)?;
                    let next_len = stack
                        .len()
                        .checked_add(2)
                        .ok_or_else(|| invalid_data("descriptor traversal stack overflows"))?;
                    if next_len > capacity {
                        return Err(invalid_data(
                            "descriptor traversal exceeds validated root depth",
                        ));
                    }
                    stack.push(concat.newer.as_ref());
                    stack.push(concat.older.as_ref());
                }
            }
        }

        if samples != root_meta.samples
            || blocks != root_meta.blocks
            || leaves != root_meta.leaves
            || nodes != root_meta.nodes
            || output
                .last()
                .is_some_and(|leaf| leaf.last != root_meta.last)
        {
            return Err(invalid_data(
                "descriptor traversal totals disagree with root metadata",
            ));
        }
        Ok(())
    }
}

fn validate_leaf(leaf: &DescriptorLeaf) -> io::Result<()> {
    let meta = &leaf.meta;
    if meta.depth != 1 || meta.leaves != 1 || meta.nodes != 1 || meta.first > meta.last {
        return Err(invalid_data("invalid descriptor leaf metadata"));
    }
    let run = leaf
        .fragment
        .run_exact(meta.identity.key.series, meta.identity.key.kind)
        .ok_or_else(|| invalid_data("descriptor leaf references a missing frozen run"))?;
    let blocks = u64::try_from(run.encoded.block_count())
        .map_err(|_| invalid_data("frozen run block count overflows u64"))?;
    if run.encoded.codec_name() != meta.identity.codec
        || run.encoded.sample_count() != meta.samples
        || blocks != meta.blocks
        || leaf.fragment.start_ms() != meta.identity.key.fragment.start_ms
        || leaf.fragment.end_ms() != meta.identity.key.fragment.end_ms
        || leaf.fragment.lane() != meta.identity.key.fragment.lane
    {
        return Err(invalid_data(
            "descriptor leaf metadata does not match its immutable fragment",
        ));
    }
    Ok(())
}

fn validate_concat(concat: &DescriptorConcat) -> io::Result<()> {
    let meta = &concat.meta;
    let older = concat.older.meta();
    let newer = concat.newer.meta();
    if older.identity.as_ref() != newer.identity.as_ref()
        || meta.identity.as_ref() != older.identity.as_ref()
        || older.last >= newer.first
        || meta.first != older.first
        || meta.last != newer.last
    {
        return Err(invalid_data("invalid descriptor concat identity or order"));
    }
    let expected_depth = older
        .depth
        .max(newer.depth)
        .checked_add(1)
        .ok_or_else(|| invalid_data("descriptor depth overflows u8"))?;
    let expected_nodes = checked_sum(older.nodes, newer.nodes, "node")?
        .checked_add(1)
        .ok_or_else(|| invalid_data("descriptor node count overflows u64"))?;
    if meta.depth != expected_depth
        || meta.depth > MAX_DESCRIPTOR_DEPTH
        || meta.samples != checked_sum(older.samples, newer.samples, "sample")?
        || meta.blocks != checked_sum(older.blocks, newer.blocks, "block")?
        || meta.leaves != checked_sum(older.leaves, newer.leaves, "leaf")?
        || meta.nodes != expected_nodes
    {
        return Err(invalid_data("invalid descriptor concat aggregate metadata"));
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct RunLevels {
    identity: Arc<DescriptorIdentity>,
    roots: Box<[(u8, Arc<DescriptorNode>)]>,
    publications: u64,
}

impl RunLevels {
    fn one(leaf: Arc<DescriptorNode>) -> Self {
        Self {
            identity: Arc::clone(&leaf.meta().identity),
            roots: vec![(0, leaf)].into_boxed_slice(),
            publications: 1,
        }
    }

    fn append(&self, leaf: Arc<DescriptorNode>) -> io::Result<Self> {
        if self.identity.as_ref() != leaf.meta().identity.as_ref() {
            return Err(invalid_data(
                "new descriptor leaf does not match existing sample key",
            ));
        }
        if self
            .roots
            .iter()
            .any(|(_, root)| root.meta().last >= leaf.meta().first)
        {
            return Err(invalid_data(
                "new descriptor leaf does not follow every visible root",
            ));
        }
        let publications = self
            .publications
            .checked_add(1)
            .ok_or_else(|| invalid_data("descriptor publication count overflows u64"))?;
        let mut roots = self.roots.to_vec();
        let mut level = 0u8;
        let mut carry = leaf;
        loop {
            match roots.binary_search_by_key(&level, |(existing, _)| *existing) {
                Ok(index) => {
                    let (_, older) = roots.remove(index);
                    carry = DescriptorNode::concat(older, carry)?;
                    level = level.checked_add(1).ok_or_else(|| {
                        invalid_data("descriptor level overflows the u64 publication bound")
                    })?;
                    if level >= MAX_DESCRIPTOR_DEPTH {
                        return Err(invalid_data(
                            "descriptor carry exceeds the u64 publication bound",
                        ));
                    }
                }
                Err(index) => {
                    roots
                        .try_reserve(1)
                        .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
                    roots.insert(index, (level, carry));
                    break;
                }
            }
        }
        let visible_bound = u64::BITS - publications.leading_zeros();
        if roots.len() > visible_bound as usize {
            return Err(invalid_data(
                "visible descriptor roots exceed the logarithmic bound",
            ));
        }
        Ok(Self {
            identity: Arc::clone(&self.identity),
            roots: roots.into_boxed_slice(),
            publications,
        })
    }

    fn append_leaves(&self, output: &mut Vec<FrozenRunRef>) -> io::Result<()> {
        let output_start = output.len();
        let mut last = None;
        let mut previous_level = None;
        for (level, root) in self.roots.iter().rev() {
            if *level >= MAX_DESCRIPTOR_DEPTH || root.meta().depth != level.saturating_add(1) {
                return Err(invalid_data(
                    "descriptor root level disagrees with its validated depth",
                ));
            }
            if root.meta().identity.as_ref() != self.identity.as_ref() {
                return Err(invalid_data(
                    "descriptor root identity disagrees with its level map",
                ));
            }
            if previous_level.is_some_and(|previous| previous <= *level) {
                return Err(invalid_data(
                    "visible descriptor levels are not unique and descending",
                ));
            }
            previous_level = Some(*level);
            if last.is_some_and(|previous| previous >= root.meta().first) {
                return Err(invalid_data(
                    "visible descriptor roots are not in sequence order",
                ));
            }
            root.append_leaves(output)?;
            last = Some(root.meta().last);
        }
        let appended = u64::try_from(output.len() - output_start)
            .map_err(|_| invalid_data("descriptor traversal leaf count overflows u64"))?;
        if appended != self.publications {
            return Err(invalid_data(
                "descriptor publication count disagrees with traversed leaves",
            ));
        }
        let visible_bound = u64::BITS - self.publications.leading_zeros();
        if self.roots.is_empty() || self.roots.len() > visible_bound as usize {
            return Err(invalid_data(
                "visible descriptor roots exceed the logarithmic bound",
            ));
        }
        Ok(())
    }
}

type MapLink = Option<Arc<SampleMapNode>>;

#[derive(Debug)]
struct SampleMapNode {
    key: LiveSampleKey,
    value: RunLevels,
    left: MapLink,
    right: MapLink,
    height: u16,
    entries: u64,
}

impl SampleMapNode {
    fn make(
        key: LiveSampleKey,
        value: RunLevels,
        left: MapLink,
        right: MapLink,
    ) -> io::Result<Arc<Self>> {
        let child_height = map_height(&left).max(map_height(&right));
        let height = child_height
            .checked_add(1)
            .ok_or_else(|| invalid_data("persistent sample-map height overflows u16"))?;
        let entries = checked_sum(map_entries(&left), map_entries(&right), "map entry")?
            .checked_add(1)
            .ok_or_else(|| invalid_data("persistent sample-map entry count overflows u64"))?;
        Ok(Arc::new(Self {
            key,
            value,
            left,
            right,
            height,
            entries,
        }))
    }
}

fn map_height(link: &MapLink) -> u16 {
    link.as_ref().map_or(0, |node| node.height)
}

fn map_entries(link: &MapLink) -> u64 {
    link.as_ref().map_or(0, |node| node.entries)
}

fn map_get<'a>(root: &'a MapLink, key: &LiveSampleKey) -> Option<&'a RunLevels> {
    let mut current = root.as_deref();
    while let Some(node) = current {
        match key.cmp(&node.key) {
            Ordering::Less => current = node.left.as_deref(),
            Ordering::Greater => current = node.right.as_deref(),
            Ordering::Equal => return Some(&node.value),
        }
    }
    None
}

fn map_insert(
    root: &MapLink,
    key: LiveSampleKey,
    value: RunLevels,
) -> io::Result<Arc<SampleMapNode>> {
    let Some(node) = root else {
        return SampleMapNode::make(key, value, None, None);
    };
    let rebuilt = match key.cmp(&node.key) {
        Ordering::Less => SampleMapNode::make(
            node.key.clone(),
            node.value.clone(),
            Some(map_insert(&node.left, key, value)?),
            node.right.clone(),
        )?,
        Ordering::Greater => SampleMapNode::make(
            node.key.clone(),
            node.value.clone(),
            node.left.clone(),
            Some(map_insert(&node.right, key, value)?),
        )?,
        Ordering::Equal => {
            return SampleMapNode::make(key, value, node.left.clone(), node.right.clone());
        }
    };
    map_balance(rebuilt)
}

fn map_remove(root: &MapLink, key: &LiveSampleKey) -> io::Result<(MapLink, bool)> {
    let Some(node) = root else {
        return Ok((None, false));
    };
    let (rebuilt, removed) = match key.cmp(&node.key) {
        Ordering::Less => {
            let (left, removed) = map_remove(&node.left, key)?;
            if !removed {
                return Ok((root.clone(), false));
            }
            (
                Some(SampleMapNode::make(
                    node.key.clone(),
                    node.value.clone(),
                    left,
                    node.right.clone(),
                )?),
                true,
            )
        }
        Ordering::Greater => {
            let (right, removed) = map_remove(&node.right, key)?;
            if !removed {
                return Ok((root.clone(), false));
            }
            (
                Some(SampleMapNode::make(
                    node.key.clone(),
                    node.value.clone(),
                    node.left.clone(),
                    right,
                )?),
                true,
            )
        }
        Ordering::Equal => match (&node.left, &node.right) {
            (None, None) => (None, true),
            (Some(_), None) => (node.left.clone(), true),
            (None, Some(_)) => (node.right.clone(), true),
            (Some(_), Some(right)) => {
                let successor = map_min(right);
                let (new_right, removed) = map_remove(&node.right, &successor.key)?;
                if !removed {
                    return Err(invalid_data(
                        "persistent sample-map successor disappeared during removal",
                    ));
                }
                (
                    Some(SampleMapNode::make(
                        successor.key.clone(),
                        successor.value.clone(),
                        node.left.clone(),
                        new_right,
                    )?),
                    true,
                )
            }
        },
    };
    match rebuilt {
        Some(node) => Ok((Some(map_balance(node)?), removed)),
        None => Ok((None, removed)),
    }
}

fn map_min(mut node: &Arc<SampleMapNode>) -> &Arc<SampleMapNode> {
    while let Some(left) = &node.left {
        node = left;
    }
    node
}

fn map_balance(node: Arc<SampleMapNode>) -> io::Result<Arc<SampleMapNode>> {
    let balance = i32::from(map_height(&node.left)) - i32::from(map_height(&node.right));
    if balance > 1 {
        let left = node
            .left
            .as_ref()
            .ok_or_else(|| invalid_data("left-heavy sample map has no left child"))?;
        if map_height(&left.left) < map_height(&left.right) {
            let rotated = map_rotate_left(Arc::clone(left))?;
            let rebuilt = SampleMapNode::make(
                node.key.clone(),
                node.value.clone(),
                Some(rotated),
                node.right.clone(),
            )?;
            return map_rotate_right(rebuilt);
        }
        return map_rotate_right(node);
    }
    if balance < -1 {
        let right = node
            .right
            .as_ref()
            .ok_or_else(|| invalid_data("right-heavy sample map has no right child"))?;
        if map_height(&right.right) < map_height(&right.left) {
            let rotated = map_rotate_right(Arc::clone(right))?;
            let rebuilt = SampleMapNode::make(
                node.key.clone(),
                node.value.clone(),
                node.left.clone(),
                Some(rotated),
            )?;
            return map_rotate_left(rebuilt);
        }
        return map_rotate_left(node);
    }
    Ok(node)
}

fn map_rotate_left(root: Arc<SampleMapNode>) -> io::Result<Arc<SampleMapNode>> {
    let pivot = root
        .right
        .as_ref()
        .ok_or_else(|| invalid_data("cannot rotate sample map left without a right child"))?;
    let left = SampleMapNode::make(
        root.key.clone(),
        root.value.clone(),
        root.left.clone(),
        pivot.left.clone(),
    )?;
    SampleMapNode::make(
        pivot.key.clone(),
        pivot.value.clone(),
        Some(left),
        pivot.right.clone(),
    )
}

fn map_rotate_right(root: Arc<SampleMapNode>) -> io::Result<Arc<SampleMapNode>> {
    let pivot = root
        .left
        .as_ref()
        .ok_or_else(|| invalid_data("cannot rotate sample map right without a left child"))?;
    let right = SampleMapNode::make(
        root.key.clone(),
        root.value.clone(),
        pivot.right.clone(),
        root.right.clone(),
    )?;
    SampleMapNode::make(
        pivot.key.clone(),
        pivot.value.clone(),
        pivot.left.clone(),
        Some(right),
    )
}

fn map_in_order(root: &MapLink) -> io::Result<Vec<(&LiveSampleKey, &RunLevels)>> {
    let capacity = root.as_ref().map_or(0, |node| usize::from(node.height));
    let mut stack = Vec::new();
    stack
        .try_reserve_exact(capacity)
        .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
    let mut entries = Vec::new();
    let expected = map_entries(root);
    entries
        .try_reserve_exact(
            usize::try_from(expected)
                .map_err(|_| invalid_data("sample-map entry count exceeds usize"))?,
        )
        .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;

    let mut current = root.as_deref();
    let mut previous_key = None;
    while current.is_some() || !stack.is_empty() {
        while let Some(node) = current {
            validate_map_node(node)?;
            if stack.len() >= capacity {
                return Err(invalid_data(
                    "sample-map traversal exceeds validated root height",
                ));
            }
            stack.push(node);
            current = node.left.as_deref();
        }
        let node = stack
            .pop()
            .ok_or_else(|| invalid_data("sample-map traversal stack underflow"))?;
        if previous_key.is_some_and(|previous| previous >= &node.key) {
            return Err(invalid_data(
                "persistent sample-map keys are not strictly ordered",
            ));
        }
        previous_key = Some(&node.key);
        entries.push((&node.key, &node.value));
        current = node.right.as_deref();
    }
    if u64::try_from(entries.len()).ok() != Some(expected) {
        return Err(invalid_data(
            "sample-map traversal disagrees with root entry count",
        ));
    }
    Ok(entries)
}

fn validate_map_node(node: &SampleMapNode) -> io::Result<()> {
    let expected_height = map_height(&node.left)
        .max(map_height(&node.right))
        .checked_add(1)
        .ok_or_else(|| invalid_data("persistent sample-map height overflows u16"))?;
    let expected_entries = checked_sum(
        map_entries(&node.left),
        map_entries(&node.right),
        "map entry",
    )?
    .checked_add(1)
    .ok_or_else(|| invalid_data("persistent sample-map entry count overflows u64"))?;
    let balance = i32::from(map_height(&node.left)) - i32::from(map_height(&node.right));
    if node.height != expected_height || node.entries != expected_entries || balance.abs() > 1 {
        return Err(invalid_data(
            "persistent sample-map node metadata or AVL balance is invalid",
        ));
    }
    Ok(())
}

fn validate_exact_fragment_certificate(
    root: &MapLink,
    fragment_count: u64,
    fragment_identities: &BTreeSet<FrozenFragmentIdentity>,
    exact_fragments: &[FrozenFragmentIdentity],
    exact_mismatch: &'static str,
) -> io::Result<()> {
    let certified_count = u64::try_from(fragment_identities.len())
        .map_err(|_| invalid_data("sample fragment certificate count exceeds u64"))?;
    if certified_count != fragment_count {
        return Err(invalid_data(
            "persistent sample fragment certificate disagrees with the store count",
        ));
    }
    if root.is_none() != (fragment_count == 0) {
        return Err(invalid_data(
            "persistent sample root emptiness disagrees with its fragment certificate",
        ));
    }
    if fragment_identities.len() != exact_fragments.len()
        || !fragment_identities.iter().eq(exact_fragments.iter())
    {
        return Err(invalid_data(exact_mismatch));
    }
    Ok(())
}

/// Aggregate structural telemetry for one immutable sample root.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LiveSampleStoreStats {
    pub keys: u64,
    pub visible_roots: u64,
    pub descriptor_nodes: u64,
    pub leaves: u64,
    pub blocks: u64,
    pub samples: u64,
    pub maximum_depth: u8,
}

/// An immutable persistent sample root.
#[derive(Debug, Default, Clone)]
pub struct LiveSampleStore {
    root: MapLink,
    fragment_count: u64,
    required_catalog_revision: u64,
    fragment_identities: Arc<BTreeSet<FrozenFragmentIdentity>>,
}

impl LiveSampleStore {
    pub fn is_empty(&self) -> bool {
        self.root.is_none()
    }

    pub fn key_count(&self) -> u64 {
        map_entries(&self.root)
    }

    pub const fn fragment_count(&self) -> u64 {
        self.fragment_count
    }

    pub const fn required_catalog_revision(&self) -> u64 {
        self.required_catalog_revision
    }

    /// Proves that the immutable sample root contains exactly the supplied
    /// sorted fragment identities.
    ///
    /// This validates both the inductively maintained certificate and its
    /// binding to the root's structural fragment count. Duplicate or unsorted
    /// input therefore fails closed rather than being treated as set equality.
    pub fn validate_exact_fragment_identities(
        &self,
        exact_fragments: &[FrozenFragmentIdentity],
    ) -> io::Result<()> {
        validate_exact_fragment_certificate(
            &self.root,
            self.fragment_count,
            &self.fragment_identities,
            exact_fragments,
            "persistent sample root does not exactly match the supplied fragment identities",
        )
    }

    pub fn contains_key(&self, key: &LiveSampleKey) -> bool {
        map_get(&self.root, key).is_some()
    }

    /// Returns the exact sorted set of series with at least one visible run.
    ///
    /// Catalog candidates use this as their activation/retirement authority.
    /// A label row that has no sample path is intentionally absent.
    pub fn active_series_refs(&self) -> io::Result<Vec<SeriesRef>> {
        let mut series = Vec::new();
        series
            .try_reserve_exact(
                usize::try_from(self.key_count())
                    .map_err(|_| invalid_data("sample-map key count exceeds usize"))?,
            )
            .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
        for (key, _) in map_in_order(&self.root)? {
            if series.last().copied() != Some(key.series) {
                series.push(key.series);
            }
        }
        series.sort_unstable();
        series.dedup();
        Ok(series)
    }

    pub fn visible_root_count(&self, key: &LiveSampleKey) -> Option<usize> {
        map_get(&self.root, key).map(|levels| levels.roots.len())
    }

    pub fn stats(&self) -> io::Result<LiveSampleStoreStats> {
        let mut stats = LiveSampleStoreStats {
            keys: self.key_count(),
            ..LiveSampleStoreStats::default()
        };
        for (_, levels) in map_in_order(&self.root)? {
            stats.visible_roots = stats
                .visible_roots
                .checked_add(
                    u64::try_from(levels.roots.len())
                        .map_err(|_| invalid_data("visible root count overflows u64"))?,
                )
                .ok_or_else(|| invalid_data("visible root count overflows u64"))?;
            for (_, root) in &levels.roots {
                let meta = root.meta();
                stats.descriptor_nodes =
                    checked_sum(stats.descriptor_nodes, meta.nodes, "descriptor node")?;
                stats.leaves = checked_sum(stats.leaves, meta.leaves, "descriptor leaf")?;
                stats.blocks = checked_sum(stats.blocks, meta.blocks, "descriptor block")?;
                stats.samples = checked_sum(stats.samples, meta.samples, "descriptor sample")?;
                stats.maximum_depth = stats.maximum_depth.max(meta.depth);
            }
        }
        Ok(stats)
    }

    pub(super) fn ordered_runs(&self, start_ms: u64, end_ms: u64) -> io::Result<Vec<FrozenRunRef>> {
        if end_ms < start_ms {
            return Ok(Vec::new());
        }
        let mut runs = Vec::new();
        for (key, levels) in map_in_order(&self.root)? {
            if key.fragment.overlaps(start_ms, end_ms) {
                levels.append_leaves(&mut runs)?;
            }
        }
        runs.sort_by(|left, right| left.read_order_key().cmp(&right.read_order_key()));
        Ok(runs)
    }

    pub(super) fn compatibility(mut fragments: Vec<Arc<FrozenHeadFragment>>) -> io::Result<Self> {
        fragments.sort_by_key(|fragment| {
            (
                fragment.start_ms(),
                fragment.end_ms(),
                fragment.lane(),
                fragment.publication_sequence(),
            )
        });
        let mut builder = LiveSampleStoreBuilder::new();
        for (index, fragment) in fragments.into_iter().enumerate() {
            let identity = FrozenFragmentIdentity::compatibility(&fragment, index)?;
            builder.insert_fragment(identity, fragment)?;
        }
        Ok(builder.finish())
    }

    #[cfg(test)]
    fn value(&self, key: &LiveSampleKey) -> Option<&RunLevels> {
        map_get(&self.root, key)
    }
}

/// A private candidate builder.  Every operation constructs a replacement
/// root; committed stores are never mutated.
#[derive(Debug, Default)]
pub struct LiveSampleStoreBuilder {
    root: MapLink,
    fragment_count: u64,
    required_catalog_revision: u64,
    fragment_identities: Arc<BTreeSet<FrozenFragmentIdentity>>,
}

impl LiveSampleStoreBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_store(store: &LiveSampleStore) -> Self {
        Self {
            root: store.root.clone(),
            fragment_count: store.fragment_count,
            required_catalog_revision: store.required_catalog_revision,
            fragment_identities: Arc::clone(&store.fragment_identities),
        }
    }

    pub fn insert_fragment(
        &mut self,
        identity: FrozenFragmentIdentity,
        fragment: Arc<FrozenHeadFragment>,
    ) -> io::Result<()> {
        if fragment.start_ms() != identity.key.start_ms
            || fragment.end_ms() != identity.key.end_ms
            || fragment.lane() != identity.key.lane
        {
            return Err(invalid_data(
                "frozen fragment bytes do not match their persistent identity",
            ));
        }
        if fragment.is_empty() {
            return Ok(());
        }
        if self.fragment_identities.contains(&identity) {
            return Err(invalid_data(
                "persistent sample root contains a duplicate frozen fragment identity",
            ));
        }

        let mut candidate = self.root.clone();
        let mut required_catalog_revision = self.required_catalog_revision;
        for run in fragment.runs.iter() {
            let key = LiveSampleKey::new(identity.key.clone(), run.series, run.kind);
            let codec = run.encoded.codec_name();
            let descriptor_identity = match map_get(&candidate, &key) {
                Some(levels) => {
                    if levels.identity.codec != codec {
                        return Err(invalid_data(
                            "sample codec changed across frozen publications",
                        ));
                    }
                    Arc::clone(&levels.identity)
                }
                None => Arc::new(DescriptorIdentity {
                    key: key.clone(),
                    codec,
                }),
            };
            let leaf = DescriptorNode::leaf(
                descriptor_identity,
                Arc::clone(&fragment),
                run,
                identity.order_range,
            )?;
            let levels = match map_get(&candidate, &key) {
                Some(levels) => levels.append(leaf)?,
                None => RunLevels::one(leaf),
            };
            candidate = Some(map_insert(&candidate, key, levels)?);
            required_catalog_revision =
                required_catalog_revision.max(u64::from(run.series.get()).saturating_add(1));
        }
        let fragment_count = self
            .fragment_count
            .checked_add(1)
            .ok_or_else(|| invalid_data("frozen fragment count overflows u64"))?;
        self.root = candidate;
        self.fragment_count = fragment_count;
        self.required_catalog_revision = required_catalog_revision;
        let inserted = Arc::make_mut(&mut self.fragment_identities).insert(identity);
        debug_assert!(inserted, "duplicate identity was rejected before insertion");
        Ok(())
    }

    /// Omits every series/kind path for one handed-off fragment identity.
    pub fn remove_fragment_key(&mut self, fragment: &FrozenFragmentKey) -> io::Result<usize> {
        let entries = map_in_order(&self.root)?;
        let keys: Vec<_> = entries
            .iter()
            .filter(|(key, _)| &key.fragment == fragment)
            .map(|(key, _)| (*key).clone())
            .collect();
        let mut retired_ranges = std::collections::BTreeSet::new();
        for (key, levels) in entries {
            if &key.fragment != fragment {
                continue;
            }
            let mut leaves = Vec::new();
            levels.append_leaves(&mut leaves)?;
            retired_ranges.extend(
                leaves
                    .into_iter()
                    .map(|leaf| (leaf.first, leaf.last, Arc::as_ptr(&leaf.fragment) as usize)),
            );
        }
        let mut candidate = self.root.clone();
        for key in &keys {
            let (root, removed) = map_remove(&candidate, key)?;
            if !removed {
                return Err(invalid_data(
                    "sample key disappeared while constructing a retirement root",
                ));
            }
            candidate = root;
        }
        let fragment_count = self
            .fragment_count
            .checked_sub(
                u64::try_from(retired_ranges.len())
                    .map_err(|_| invalid_data("retired fragment count overflows u64"))?,
            )
            .ok_or_else(|| invalid_data("retired fragment count exceeds the store total"))?;
        let retired_identities = self
            .fragment_identities
            .iter()
            .filter(|identity| identity.fragment_key() == fragment)
            .count();
        if retired_identities != retired_ranges.len() {
            return Err(invalid_data(
                "persistent sample fragment certificate disagrees with retired descriptor leaves",
            ));
        }
        Arc::make_mut(&mut self.fragment_identities)
            .retain(|identity| identity.fragment_key() != fragment);
        self.root = candidate;
        self.fragment_count = fragment_count;
        Ok(keys.len())
    }

    /// Replaces the candidate with an empty sample root after proving that
    /// every fragment in the committed root is part of the exact handoff.
    ///
    /// The identity certificate is maintained inductively alongside every
    /// insert/removal. Comparing it avoids one full sample-map traversal per
    /// handed fragment during the final sealed-only shutdown publication.
    /// The historical required catalog revision is retained so the empty
    /// successor cannot weaken its label-snapshot cut.
    pub fn clear_if_exact_fragments(
        &mut self,
        exact_committed_fragments: &[FrozenFragmentIdentity],
    ) -> io::Result<()> {
        validate_exact_fragment_certificate(
            &self.root,
            self.fragment_count,
            &self.fragment_identities,
            exact_committed_fragments,
            "final empty sample root does not exactly cover every committed fragment",
        )?;
        self.root = None;
        self.fragment_count = 0;
        self.fragment_identities = Arc::default();
        Ok(())
    }

    pub fn finish(self) -> LiveSampleStore {
        LiveSampleStore {
            root: self.root,
            fragment_count: self.fragment_count,
            required_catalog_revision: self.required_catalog_revision,
            fragment_identities: self.fragment_identities,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct FrozenRunRef {
    key: LiveSampleKey,
    first: RecordedSampleOrder,
    last: RecordedSampleOrder,
    fragment: Arc<FrozenHeadFragment>,
}

impl FrozenRunRef {
    pub(super) fn key(&self) -> &LiveSampleKey {
        &self.key
    }

    pub(super) fn fragment(&self) -> &FrozenHeadFragment {
        &self.fragment
    }

    pub(super) fn run(&self) -> io::Result<&FrozenSeriesRun> {
        self.fragment
            .run_exact(self.key.series, self.key.kind)
            .ok_or_else(|| invalid_data("persistent descriptor references a missing frozen run"))
    }

    fn read_order_key(
        &self,
    ) -> (
        u64,
        u64,
        FrozenHeadLane,
        &LivePartitionKey,
        RecordedSampleOrder,
        RecordedSampleOrder,
        SeriesRef,
        SampleKind,
    ) {
        (
            self.key.fragment.start_ms,
            self.key.fragment.end_ms,
            self.key.fragment.lane,
            &self.key.fragment.partition,
            self.first,
            self.last,
            self.key.series,
            self.key.kind,
        )
    }
}

fn checked_order_range(
    first: RecordedSampleOrder,
    last: RecordedSampleOrder,
) -> io::Result<RecordedSampleOrderRange> {
    if first > last {
        return Err(invalid_data(
            "frozen fragment recorded order range is reversed",
        ));
    }
    if first == last {
        Ok(RecordedSampleOrderRange::one(first))
    } else {
        RecordedSampleOrderRange::one(first).checked_extend(last)
    }
}

fn checked_sum(left: u64, right: u64, subject: &str) -> io::Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| invalid_data(format!("descriptor {subject} count overflows u64")))
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests;
