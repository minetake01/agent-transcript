use txcript::Span;

/// 1-based inclusive bounds. Either end may be open (`#5-`, `#-10`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpanReq {
    pub start: Option<usize>,
    pub end: Option<usize>,
}

pub fn parse_ref(input: &str) -> (&str, Option<SpanReq>) {
    match input.rfind('#') {
        None => (input, None),
        Some(pos) => {
            let (source, suffix) = (&input[..pos], &input[pos + 1..]);
            match (source.is_empty(), parse_range(suffix)) {
                (true, _) | (false, None) => (input, None),
                (false, Some(req)) => (source, Some(req)),
            }
        }
    }
}

fn parse_range(input: &str) -> Option<SpanReq> {
    fn bound(text: &str) -> Option<Option<usize>> {
        match text {
            "" => Some(None),
            digits if digits.bytes().all(|byte| byte.is_ascii_digit()) => {
                digits.parse().ok().map(Some)
            }
            _ => None,
        }
    }
    match input.split_once('-') {
        None => bound(input)?.map(|number| SpanReq {
            start: Some(number),
            end: Some(number),
        }),
        Some(("", "")) => None,
        Some((start, end)) => Some(SpanReq {
            start: bound(start)?,
            end: bound(end)?,
        }),
    }
}

impl SpanReq {
    pub fn resolve(&self, len: usize) -> Result<Span, String> {
        let start = self.start.unwrap_or(1);
        let end = self.end.unwrap_or(len);
        match (start, end) {
            (0, _) | (_, 0) => Err(format!("message numbers are 1-based — `{self}` has a 0")),
            (start, end) if start > end => Err(format!("range `{self}` is inverted")),
            (_, end) if end > len => Err(format!(
                "range `{self}` is out of bounds — the session has {len} messages"
            )),
            (start, end) => Ok(Span(start - 1..end)),
        }
    }
}

impl std::fmt::Display for SpanReq {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.start, self.end) {
            (Some(start), Some(end)) if start == end => write!(formatter, "#{start}"),
            (start, end) => {
                write!(formatter, "#")?;
                if let Some(start) = start {
                    write!(formatter, "{start}")?;
                }
                write!(formatter, "-")?;
                if let Some(end) = end {
                    write!(formatter, "{end}")?;
                }
                Ok(())
            }
        }
    }
}

pub fn format_span(span: &Span) -> String {
    match span.0.len() {
        1 => format!("#{}", span.0.start + 1),
        _ => format!("#{}-{}", span.0.start + 1, span.0.end),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_txcript_range_suffixes() {
        assert_eq!(parse_ref("abc").1, None);
        let (_, range) = parse_ref("abc#5-12");
        assert_eq!(range.unwrap().resolve(20).unwrap(), Span(4..12));
        assert_eq!(
            parse_ref("abc#7").1.unwrap().resolve(20).unwrap(),
            Span(6..7)
        );
        assert_eq!(
            parse_ref("abc#5-").1.unwrap().resolve(20).unwrap(),
            Span(4..20)
        );
        assert_eq!(
            parse_ref("abc#-10").1.unwrap().resolve(20).unwrap(),
            Span(0..10)
        );
        assert_eq!(parse_ref("title#anchor").1, None);
    }
}
