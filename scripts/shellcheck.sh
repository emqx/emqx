#!/bin/bash
set -euo pipefail

target_files=()
while IFS='' read -r line; do target_files+=("$line"); done < <(grep -r -l --exclude-dir=.git --exclude-dir=_build "#!/bin/" .)
return_code=0
for i in "${target_files[@]}"; do
  echo checking "$i" ...
  if ! shellcheck "$i"; then
    return_code=1
  fi
done

exit $return_code
