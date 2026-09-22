use txcript::{Common, Span, Transcript};

use crate::fragment::{format_span, SpanReq};

pub const READ_BUDGET: usize = 100_000;

pub fn render(
    src: &str,
    transcript: &Transcript<Common>,
    span_req: Option<&SpanReq>,
) -> std::result::Result<String, String> {
    let total = transcript.body.len();
    let span = match span_req {
        Some(req) => req.resolve(total)?,
        None => Span(0..total),
    };
    let rendered = txcript::text::to_text_fragment(transcript, &span)
        .ok_or_else(|| format!("range is out of bounds — the session has {total} messages"))?;
    if rendered.len() > READ_BUDGET && span.0.len() > 1 {
        return Err(over_budget(src, transcript, &span, rendered.len()));
    }
    Ok(rendered)
}

fn over_budget(src: &str, transcript: &Transcript<Common>, span: &Span, rendered: usize) -> String {
    let sizes: Vec<usize> = span
        .0
        .clone()
        .map(|index| {
            txcript::text::to_text_fragment(transcript, &Span(index..index + 1))
                .map_or(0, |text| text.len())
        })
        .collect();
    let chunks = chunk_ranges(&sizes, span.0.start, READ_BUDGET);
    let shown = chunks
        .iter()
        .take(12)
        .map(|range| match range.len() {
            1 => format!("`{src}#{}`", range.start + 1),
            _ => format!("`{src}#{}-{}`", range.start + 1, range.end),
        })
        .collect::<Vec<_>>()
        .join(", ");
    let more = if chunks.len() > 12 { ", …" } else { "" };
    format!(
        "session `{src}` renders to {rendered} bytes, over the {READ_BUDGET}-byte read budget — read it in ranges: {shown}{more}"
    )
}

fn chunk_ranges(sizes: &[usize], start: usize, budget: usize) -> Vec<std::ops::Range<usize>> {
    let mut chunks = Vec::new();
    let mut lo = 0usize;
    let mut acc = 0usize;
    for (index, &size) in sizes.iter().enumerate() {
        if index > lo && acc + size > budget {
            chunks.push(start + lo..start + index);
            lo = index;
            acc = 0;
        }
        acc += size;
    }
    if lo < sizes.len() {
        chunks.push(start + lo..start + sizes.len());
    }
    chunks
}

pub fn suggest_span(span: &Span) -> String {
    format_span(span)
}
