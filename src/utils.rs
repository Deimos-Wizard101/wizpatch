use crate::errors::WizPatchError;

/// Extracts the revision identifier from a patch URL.
///
/// Patch URLs contain `WizPatcher/<revision>/...`; this returns `<revision>`.
pub fn revision_from_url(url: &str) -> Result<String, WizPatchError> {
    let marker = "WizPatcher/";
    let start = url
        .find(marker)
        .ok_or_else(|| WizPatchError::NoRevision(url.to_string()))?
        + marker.len();
    let rest = &url[start..];
    let end = rest.find('/').unwrap_or(rest.len());
    Ok(rest[..end].to_string())
}

/// Strips a leading platform-prefix segment (`Windows`, `MacOS`, `Steam`) from a
/// record's `SrcFileName` so it can be joined with the local game directory.
/// Mirrors MilkLauncher's `fix_src_path`.
pub fn fix_src_path(path: &str) -> &str {
    let mut parts = path.splitn(2, '/');
    let first = parts.next().unwrap_or("");
    let rest = parts.next();
    let lower = first.to_ascii_lowercase();
    if matches!(lower.as_str(), "windows" | "macos" | "steam") {
        rest.unwrap_or(first)
    } else {
        path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_revision() {
        let url = "http://example.com/WizPatcher/V_r777777.WizardDev/Live/_FileList.bin";
        assert_eq!(revision_from_url(url).unwrap(), "V_r777777.WizardDev");
    }
}
