from __future__ import annotations

import zlib

from scholarpath.search.official_direct_fetch import (
    _extract_html_and_links_from_bytes,
    _extract_pdf_text,
    _normalize_url,
)


def test_extract_html_and_links_from_bytes():
    html = b"""
    <html>
      <head><title>Admissions</title></head>
      <body>
        <h1>Admissions</h1>
        <p>Acceptance rate 8%</p>
        <a href="/admissions">Admissions</a>
      </body>
    </html>
    """
    text, links = _extract_html_and_links_from_bytes(html)
    assert "Admissions" in text
    assert "Acceptance rate 8%" in text
    assert links == [("/admissions", "Admissions")]


def test_extract_pdf_text_from_flate_stream():
    stream = zlib.compress(b"BT (Acceptance Rate 8%) Tj ET")
    pdf_bytes = b"stream\n" + stream + b"\nendstream"
    text = _extract_pdf_text(pdf_bytes)
    assert "Acceptance Rate 8%" in text


def test_normalize_url_adds_scheme_and_strips_fragment():
    assert _normalize_url("example.edu/admissions#top") == "https://example.edu/admissions"
