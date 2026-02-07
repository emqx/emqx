#!/usr/bin/env bash
set -euo pipefail

cd -P -- "$(dirname -- "$0")/.."

# Production API modules backed by dashboard swagger.
mapfile -t api_files < <(rg -l "emqx_dashboard_swagger:spec\\(" apps | rg -v "/test/|\\.erl_$" | sort)

if [ ${#api_files[@]} -eq 0 ]; then
  echo "No dashboard swagger API files found"
  exit 0
fi

errors=0

echo "[check-api-summary-i18n] Checking hardcoded endpoint summaries..."
for f in "${api_files[@]}"; do
  # Flag hardcoded summaries in endpoint method maps where i18n desc/description is already present nearby.
  if perl -ne '
    our @buf;
    push @buf, $_;
    shift @buf if @buf > 20;
    next if /^\s*%%/;
    if (/^(\s*)summary\s*=>\s*<<.*>>/) {
      my $indent = length($1);
      next if $indent > 20;
      my $ctx = join("", @buf);
      if ($ctx =~ /tags\s*=>/s && $ctx =~ /(desc|description)\s*=>\s*\?DESC\(/s) {
        print "$ARGV:$.: $_";
        $::bad = 1;
      }
    }
    END { exit($::bad ? 2 : 0) }
  ' "$f"; then
    :
  else
    errors=1
  fi

done

echo "[check-api-summary-i18n] Collecting summary i18n keys..."
keys_file=$(mktemp)
for f in "${api_files[@]}"; do
  perl -ne '
    sub braces_delta {
      my ($s) = @_;
      my $open = () = $s =~ /\{/g;
      my $close = () = $s =~ /\}/g;
      return $open - $close;
    }

    sub emit_args {
      my ($kind, $block) = @_;
      while ($block =~ /\b$kind\s*=>\s*\?DESC\(([^\)]*)\)/g) {
        print "$1\n";
      }
    }

    sub analyze_block {
      my ($block) = @_;
      return if $block !~ /tags\s*=>/s;
      if ($block =~ /\bsummary\s*=>\s*\?DESC\(/s) {
        emit_args("summary", $block);
      } elsif ($block =~ /\b(desc|description)\s*=>\s*\?DESC\(/s) {
        emit_args("desc", $block);
        emit_args("description", $block);
      }
    }

    if (!$in_block) {
      next if /^\s*%%/;
      if (/^(\s*)(get|post|put|delete|patch|head|options)\s*=>\s*#\{/) {
        my $indent = length($1);
        if ($indent <= 20) {
          $in_block = 1;
          $block = "";
          $depth = 0;
        }
      }
    }

    if ($in_block) {
      my $line = $_;
      $line =~ s/%%.*$//;
      $block .= $line;
      $depth += braces_delta($line);
      if ($depth <= 0) {
        analyze_block($block);
        $in_block = 0;
        $block = "";
        $depth = 0;
      }
    }
  ' "$f"
done \
  | perl -ne '
      my $a = $_;
      chomp($a);
      $a =~ s/\s+//g;
      if ($a =~ /^"([^"]+)"$/) {
        print "ONE:$1\n";
      } elsif ($a =~ /^([a-zA-Z0-9_]+)$/) {
        print "ONE:$1\n";
      } elsif ($a =~ /^([a-zA-Z0-9_]+),"([^"]+)"$/) {
        print "TWO:$1:$2\n";
      } elsif ($a =~ /^([a-zA-Z0-9_]+),([a-zA-Z0-9_]+)$/) {
        print "TWO:$1:$2\n";
      } else {
        print "UNK:$a\n";
      }
    ' | sort -u > "$keys_file"

echo "[check-api-summary-i18n] Verifying labels exist and have no trailing period..."
while IFS= read -r line; do
  kind=${line%%:*}
  case "$kind" in
    ONE)
      key=${line#ONE:}
      if ! rg -n -U "\\b${key}\\.label\\b|\\b${key}\\s*\\{(?s).*?\\blabel\\s*:" rel/i18n >/dev/null; then
        echo "ERROR: missing i18n label for summary key '${key}'"
        errors=1
        continue
      fi
      if rg -n "${key}\\.label\\s*[:=].*\.\\s*\"\"\"?$|${key}\\.label\\s*[:=]\\s*\".*\\.\"\\s*$" -S rel/i18n >/dev/null; then
        echo "ERROR: label for summary key '${key}' ends with a period"
        errors=1
      fi
      ;;
    TWO)
      mod=$(echo "$line" | cut -d: -f2)
      key=$(echo "$line" | cut -d: -f3)
      f="rel/i18n/${mod}.hocon"
      if [ ! -f "$f" ] || ! rg -n -U "\\b${key}\\.label\\b|\\b${key}\\s*\\{(?s).*?\\blabel\\s*:" "$f" >/dev/null; then
        echo "ERROR: missing i18n label for summary key '${mod}:${key}'"
        errors=1
        continue
      fi
      if rg -n "${key}\\.label\\s*[:=].*\.\\s*\"\"\"?$|${key}\\.label\\s*[:=]\\s*\".*\\.\"\\s*$" -S "$f" >/dev/null; then
        echo "ERROR: label for summary key '${mod}:${key}' ends with a period"
        errors=1
      fi
      ;;
    UNK)
      echo "ERROR: unsupported ?DESC syntax in summary: ${line#UNK:}"
      errors=1
      ;;
  esac

done < "$keys_file"

rm -f "$keys_file"

if [ "$errors" -ne 0 ]; then
  echo "[check-api-summary-i18n] FAILED"
  exit 1
fi

echo "[check-api-summary-i18n] OK"
