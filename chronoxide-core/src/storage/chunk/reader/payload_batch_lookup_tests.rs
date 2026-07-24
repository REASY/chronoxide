use super::*;

fn batch(spans: Vec<ChunkPayloadSpan>) -> ChunkPayloadBatch {
    let physical_bytes_read = spans.iter().fold(0u64, |total, span| {
        total.saturating_add(span.bytes.len() as u64)
    });
    ChunkPayloadBatch {
        spans,
        physical_bytes_read,
    }
}

#[test]
fn decoder_cursor_finds_many_disjoint_spans_in_any_query_order() {
    const SPAN_COUNT: usize = 257;
    let spans = (0..SPAN_COUNT)
        .map(|index| ChunkPayloadSpan {
            file_id: 0,
            offset: (index as u64) * 16,
            bytes: vec![0x80, (index % 251) as u8, 0x81],
        })
        .collect::<Vec<_>>();
    assert!(chunk_payload_spans_are_sorted_and_disjoint(&spans));
    let payloads = batch(spans);

    let ascending = (0..SPAN_COUNT).collect::<Vec<_>>();
    let descending = (0..SPAN_COUNT).rev().collect::<Vec<_>>();
    let permuted = (0..SPAN_COUNT)
        .step_by(2)
        .chain((1..SPAN_COUNT).step_by(2))
        .collect::<Vec<_>>();
    for order in [ascending, descending, permuted] {
        let mut decoder = payloads.decoder();
        for index in order {
            assert_eq!(
                decoder.slice(0, (index as u64) * 16 + 1, 1).unwrap(),
                &[(index % 251) as u8]
            );
        }
    }
    let mut decoder = payloads.decoder();
    let cross_span = decoder.slice(0, 2, 15).unwrap_err();
    assert_eq!(cross_span.kind(), io::ErrorKind::InvalidData);
    let wrong_file = decoder.slice(1, 0, 1).unwrap_err();
    assert_eq!(wrong_file.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn decoder_cursor_preserves_boundaries_and_missing_range_errors() {
    let payloads = batch(vec![
        ChunkPayloadSpan {
            file_id: 0,
            offset: 10,
            bytes: vec![10, 11, 12],
        },
        ChunkPayloadSpan {
            file_id: 0,
            offset: 20,
            bytes: vec![20, 21],
        },
    ]);
    let mut decoder = payloads.decoder();

    assert_eq!(decoder.slice(0, 10, 3).unwrap(), &[10, 11, 12]);
    assert_eq!(decoder.slice(0, 11, 2).unwrap(), &[11, 12]);
    assert_eq!(decoder.slice(0, 13, 0).unwrap(), &[] as &[u8]);
    assert_eq!(decoder.slice(0, 20, 2).unwrap(), &[20, 21]);

    for (offset, len) in [(9, 1), (12, 2), (13, 1), (19, 1), (21, 2)] {
        let error = decoder.slice(0, offset, len).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            error.to_string(),
            "chunk payload request missing from batch"
        );
    }
    let wrong_file = decoder.slice(1, 10, 1).unwrap_err();
    assert_eq!(wrong_file.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        wrong_file.to_string(),
        "chunk payload request missing from batch"
    );
}

#[test]
fn decoder_cursor_preserves_range_and_span_overflow_errors() {
    let payloads = batch(vec![ChunkPayloadSpan {
        file_id: 0,
        offset: u64::MAX,
        bytes: vec![1],
    }]);
    let mut decoder = payloads.decoder();

    let range_error = decoder.slice(0, u64::MAX, 1).unwrap_err();
    assert_eq!(range_error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(range_error.to_string(), "chunk payload range overflows");

    let span_error = decoder.slice(0, u64::MAX, 0).unwrap_err();
    assert_eq!(span_error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(span_error.to_string(), "chunk payload span overflows");
}

#[test]
fn decoder_cursor_keeps_independent_positions_for_both_payload_files() {
    const SPANS_PER_FILE: usize = 129;
    let regular = (0..SPANS_PER_FILE).map(|index| ChunkPayloadSpan {
        file_id: 0,
        offset: (index as u64) * 16,
        bytes: vec![(index % 251) as u8],
    });
    let ooo = (0..SPANS_PER_FILE).map(|index| ChunkPayloadSpan {
        file_id: 1,
        offset: (index as u64) * 16,
        bytes: vec![((index + 100) % 251) as u8],
    });
    let mut payloads = batch(regular.collect());
    payloads.append(batch(ooo.collect()));
    let mut decoder = payloads.decoder();

    assert_eq!(decoder.slice(1, 128 * 16, 1).unwrap(), &[228]);
    assert_eq!(decoder.slice(0, 0, 1).unwrap(), &[0]);
    assert_eq!(decoder.slice(1, 0, 1).unwrap(), &[100]);
    assert_eq!(decoder.slice(0, 128 * 16, 1).unwrap(), &[128]);
    assert_eq!(decoder.span_cursors, [Some(128), Some(129)]);
}

#[test]
fn decoder_cursor_handles_only_ooo_invalid_file_and_recovery_after_missing_lookup() {
    let payloads = batch(vec![ChunkPayloadSpan {
        file_id: 1,
        offset: 32,
        bytes: vec![7, 8],
    }]);
    let mut decoder = payloads.decoder();

    for file_id in [0, 2] {
        let error = decoder.slice(file_id, 32, 1).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            error.to_string(),
            "chunk payload request missing from batch"
        );
    }
    let before_span = decoder.slice(1, 31, 1).unwrap_err();
    assert_eq!(
        before_span.to_string(),
        "chunk payload request missing from batch"
    );
    assert_eq!(decoder.slice(1, 33, 1).unwrap(), &[8]);
    let backward_missing = decoder.slice(1, 31, 1).unwrap_err();
    assert_eq!(
        backward_missing.to_string(),
        "chunk payload request missing from batch"
    );
    assert_eq!(decoder.slice(1, 32, 2).unwrap(), &[7, 8]);
}
