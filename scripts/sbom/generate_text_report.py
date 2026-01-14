#!/usr/bin/env python3
"""
Generate a human-readable text report from SPDX SBOM using spdx-tools.

This script converts an SPDX JSON SBOM to SPDX tag-value format using
the standard spdx-tools library.
"""

import sys
from pathlib import Path

# Import spdx-tools
try:
    from spdx_tools.spdx.parser import parse_anything
    from spdx_tools.spdx.writer.tagvalue.tagvalue_writer import write_document_to_stream
except ImportError:
    print("Error: spdx-tools is not installed. Please install it with: pip install spdx-tools", file=sys.stderr)
    sys.exit(1)


def generate_text_report(sbom_path: Path, output_path: Path = None) -> str:
    """Generate human-readable text report from SPDX SBOM using spdx-tools.
    
    Args:
        sbom_path: Path to SPDX JSON file
        output_path: Optional output path for text file
    
    Returns:
        Text report content as string
    
    Raises:
        Exception if spdx-tools fails to parse or convert the SPDX document
    """
    import io
    
    # Parse SPDX JSON file using spdx-tools
    doc = parse_anything.parse_file(str(sbom_path))
    
    # Write as tag-value format
    output = io.StringIO()
    write_document_to_stream(doc, output)
    report_text = output.getvalue()
    
    # Write to file if output path provided
    if output_path:
        with open(output_path, 'w') as f:
            f.write(report_text)
        print(f"Text report (SPDX tag-value format) written to: {output_path}", file=sys.stderr)
    
    return report_text


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate human-readable text report from SPDX SBOM using spdx-tools",
        epilog="This script uses the standard spdx-tools library to convert SPDX JSON to tag-value format."
    )
    parser.add_argument("input", type=Path, help="Input SPDX JSON file")
    parser.add_argument("-o", "--output", type=Path, help="Output text file (default: input with .txt extension)")

    args = parser.parse_args()

    if not args.input.exists():
        print(f"Error: Input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    output_path = args.output
    if not output_path:
        output_path = args.input.parent / f"{args.input.stem}.txt"

    try:
        generate_text_report(args.input, output_path)
        print(f"Successfully generated text report: {output_path}", file=sys.stderr)
    except Exception as e:
        print(f"Error: Failed to generate text report using spdx-tools: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
