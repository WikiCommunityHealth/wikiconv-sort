# wikiconv-crunch

Crunch the WikiConv dataset.

## Usage Example

```bash
$ python3 -m wikiconv-crunch --output-compression gzip \
      input/WikiConv/wikiconv-en-*.gz output \
      filter-pageid --start-id 0 --end-id 200000
```

## License

This project is realease unde GPL v3 (or later).

```plain
graphsnapshot: process links and other data extracted from the Wikipedia dump.

Copyright (C) 2020 Critian Consonni for:
* Eurecat - Centre Tecnològic de Catalunya

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
```

See the LICENSE file in this repository for further details.

