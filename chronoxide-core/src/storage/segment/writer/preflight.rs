use super::*;

/// Refuses to append a segment schema that differs from any live segment in
/// the authoritative manifest. This intentionally validates only each small,
/// fixed-size footer and its CRC; complete tracked-file checksum validation is
/// a separate read-side operation.
pub(super) fn preflight_existing_store_schema(config: &SegmentWriterConfig) -> io::Result<()> {
    let segments_dir = &config.segments_dir;
    let metadata = match fs::metadata(segments_dir) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "segment writer output root is not a directory: {}",
                segments_dir.display()
            ),
        ));
    }

    let Some(inventory) = read_manifest_inventory(segments_dir.join("manifest"))? else {
        return reject_manifestless_segment_root(segments_dir);
    };
    let expected_schema_version = config.storage_schema.footer_version();
    for segment in inventory.segments {
        let segment_dir = segments_dir.join(&segment.segment_id);
        read_segment_footer_for_exact_schema(&segment_dir, expected_schema_version).map_err(
            |error| {
                io::Error::new(
                    error.kind(),
                    format!(
                        "segment writer schema preflight failed for {} with configured footer schema {}: {error}",
                        segment_dir.display(),
                        expected_schema_version
                    ),
                )
            },
        )?;
    }
    Ok(())
}

fn reject_manifestless_segment_root(segments_dir: &Path) -> io::Result<()> {
    for entry in fs::read_dir(segments_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        if name.to_string_lossy().starts_with("seg-") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "segment writer refuses manifestless segment path {}; repair the manifest or replay into a fresh output root",
                    entry.path().display()
                ),
            ));
        }
    }
    Ok(())
}
