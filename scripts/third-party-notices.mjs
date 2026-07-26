import {createHash} from 'node:crypto'
import {readFile, readdir, writeFile} from 'node:fs/promises'
import path from 'node:path'
import {build} from 'esbuild'

const NOTICE_PATH = 'THIRD_PARTY_NOTICES.md'
const ENTRIES = ['main', 'daemon', 'post']
const LICENSE_FILE = /^(?:licen[cs]e|notice)(?:\..+)?$/i

function compareText(left, right) {
  return left < right ? -1 : left > right ? 1 : 0
}

// The 3.0.0 npm tarball declares MIT but omits its repository LICENSE. This is
// the license at that release's gitHead d2070d76a8ba07e6c7fa142caeb51ffd756e47eb.
const NODABLE_MIT = `MIT License

Copyright (c) 2026 Nodable

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.`

const GOOGLE_BSD = `Copyright 2008 Google Inc.  All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.
* Neither the name of Google Inc. nor the names of its contributors may be
  used to endorse or promote products derived from this software without
  specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.`

const WIRTZ_BSD = `Copyright (c) 2016, Daniel Wirtz  All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.
* Neither the name of its author, nor the names of its contributors may be
  used to endorse or promote products derived from this software without
  specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.`

const JOYENT_MIT = `Copyright Joyent, Inc. and other Node contributors. All rights reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.`

const WS_MIT = `Copyright (c) 2011 Einar Otto Stangvik <einaros@gmail.com>
Copyright (c) 2013 Arnout Kazemier and contributors
Copyright (c) 2016 Luigi Pinca and contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.`

function packageRoot(input) {
  const marker = 'node_modules/'
  const index = input.lastIndexOf(marker)
  if (index === -1) return undefined
  const prefix = input.slice(0, index + marker.length)
  const parts = input.slice(index + marker.length).split('/')
  const name = parts[0]?.startsWith('@') ? `${parts[0]}/${parts[1]}` : parts[0]
  return name ? {name, directory: `${prefix}${name}`} : undefined
}

async function bundledPackages() {
  const packages = new Map()
  for (const entry of ENTRIES) {
    const result = await build({
      entryPoints: [`src/${entry}.ts`],
      bundle: true,
      format: 'esm',
      legalComments: 'inline',
      metafile: true,
      platform: 'node',
      target: 'node24',
      write: false
    })
    for (const input of Object.keys(result.metafile.inputs)) {
      const dependency = packageRoot(input)
      if (dependency !== undefined) {
        packages.set(dependency.directory, dependency)
      }
    }
  }
  return [...packages.values()].sort((left, right) =>
    compareText(left.name, right.name)
  )
}

function addDocument(groups, label, content) {
  const normalized = content.trim()
  const digest = createHash('sha256').update(normalized).digest('hex')
  const group = groups.get(digest) ?? {labels: [], content: normalized}
  group.labels.push(label)
  groups.set(digest, group)
}

async function render() {
  const dependencies = await bundledPackages()
  const inventory = []
  const groups = new Map()

  for (const dependency of dependencies) {
    const metadata = JSON.parse(
      await readFile(path.join(dependency.directory, 'package.json'), 'utf8')
    )
    const label = `${metadata.name}@${metadata.version}`
    inventory.push(`- ${label} — ${String(metadata.license ?? 'unspecified')}`)

    const files = (await readdir(dependency.directory, {recursive: true}))
      .filter(file => LICENSE_FILE.test(path.basename(file)))
      .sort()
    if (files.length === 0) {
      if (label === '@nodable/entities@3.0.0') {
        addDocument(
          groups,
          `${label} (upstream repository license)`,
          NODABLE_MIT
        )
        continue
      }
      throw new Error(`${label} did not include a license or notice file`)
    }
    for (const file of files) {
      addDocument(
        groups,
        label,
        await readFile(path.join(dependency.directory, file), 'utf8')
      )
    }
  }

  const protobuf = inventory.find(line =>
    line.startsWith('- @protobuf-ts/runtime@')
  )
  if (protobuf !== undefined) {
    const label = protobuf.slice(2, protobuf.indexOf(' —'))
    addDocument(groups, `${label} (Google varint code)`, GOOGLE_BSD)
    addDocument(groups, `${label} (protobuf.js UTF-8 code)`, WIRTZ_BSD)
  }

  const actionsExec = inventory.find(line =>
    line.startsWith('- @actions/exec@')
  )
  if (actionsExec !== undefined) {
    const label = actionsExec.slice(2, actionsExec.indexOf(' —'))
    addDocument(groups, `${label} (Node/libuv quoting code)`, JOYENT_MIT)
  }

  const undici = inventory.find(line => line.startsWith('- undici@'))
  if (undici !== undefined) {
    const label = undici.slice(2, undici.indexOf(' —'))
    addDocument(groups, `${label} (ws-influenced WebSocket code)`, WS_MIT)
  }

  const sections = [...groups.values()]
    .sort((left, right) => compareText(left.labels[0], right.labels[0]))
    .map(
      (group, index) =>
        `## License document ${index + 1}\n\nApplies to:\n\n${group.labels
          .sort()
          .map(label => `- ${label}`)
          .join('\n')}\n\n\`\`\`text\n${group.content}\n\`\`\``
    )

  return `# Third-party notices

<!-- Generated by scripts/third-party-notices.mjs. Do not edit directly. -->

The JavaScript action bundles contain the following npm packages. Versions and
license identifiers come from the installed packages locked by
\`package-lock.json\`. The complete license and attribution texts follow the
inventory. Vendored Cache v2 protocol sources retain their upstream license and
commit metadata under \`src/vendor/actions-toolkit/\`.

${inventory.join('\n')}

${sections.join('\n\n')}
`
}

const expected = await render()
if (process.argv.includes('--write')) {
  await writeFile(NOTICE_PATH, expected)
} else {
  const current = await readFile(NOTICE_PATH, 'utf8').catch(() => '')
  if (current !== expected) {
    throw new Error(
      `${NOTICE_PATH} is stale; run "npm run notices:generate" and commit it`
    )
  }
}
