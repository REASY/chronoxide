#[derive(Debug, Clone, Copy)]
pub(crate) struct BufferRef {
    page: u32,
    offset: u32,
    len: u32,
}

impl BufferRef {
    pub(crate) fn len(self) -> usize {
        self.len as usize
    }
}

#[derive(Debug)]
struct ArenaPage {
    buf: Vec<u8>,
    used: usize,
}

impl ArenaPage {
    fn new(size: usize) -> Self {
        Self {
            buf: vec![0u8; size],
            used: 0,
        }
    }

    fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.used)
    }
}

#[derive(Debug)]
pub(crate) struct BlockArena {
    pages: Vec<ArenaPage>,
    page_size: usize,
}

impl BlockArena {
    pub(crate) fn new(page_size: usize) -> Self {
        Self {
            pages: Vec::new(),
            page_size: page_size.max(1),
        }
    }

    pub(crate) fn alloc(&mut self, len: usize) -> BufferRef {
        let len = len.max(1);
        if self.pages.is_empty() || self.pages.last().unwrap().remaining() < len {
            let size = self.page_size.max(len);
            self.pages.push(ArenaPage::new(size));
        }

        let page_index = self.pages.len() - 1;
        let page = self.pages.last_mut().unwrap();
        let offset = page.used;
        page.used = page.used.saturating_add(len);

        debug_assert!(page_index <= u32::MAX as usize);
        debug_assert!(offset <= u32::MAX as usize);
        debug_assert!(len <= u32::MAX as usize);

        BufferRef {
            page: page_index as u32,
            offset: offset as u32,
            len: len as u32,
        }
    }

    pub(crate) fn write(&mut self, data: &[u8]) -> BufferRef {
        if data.is_empty() {
            return self.alloc(0);
        }
        let buf_ref = self.alloc(data.len());
        self.slice_mut(buf_ref).copy_from_slice(data);
        buf_ref
    }

    pub(crate) fn slice(&self, buf_ref: BufferRef) -> &[u8] {
        let page = &self.pages[buf_ref.page as usize];
        let offset = buf_ref.offset as usize;
        let len = buf_ref.len as usize;
        &page.buf[offset..offset + len]
    }

    pub(crate) fn slice_mut(&mut self, buf_ref: BufferRef) -> &mut [u8] {
        let page = &mut self.pages[buf_ref.page as usize];
        let offset = buf_ref.offset as usize;
        let len = buf_ref.len as usize;
        &mut page.buf[offset..offset + len]
    }

    pub(crate) fn total_capacity_bytes(&self) -> usize {
        self.pages
            .iter()
            .fold(0usize, |acc, page| acc.saturating_add(page.buf.len()))
    }

    pub(crate) fn total_used_bytes(&self) -> usize {
        self.pages
            .iter()
            .fold(0usize, |acc, page| acc.saturating_add(page.used))
    }

    pub(crate) fn slack_bytes(&self) -> usize {
        self.total_capacity_bytes()
            .saturating_sub(self.total_used_bytes())
    }

    pub(crate) fn page_count(&self) -> usize {
        self.pages.len()
    }
}
