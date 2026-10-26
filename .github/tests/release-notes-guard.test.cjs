// Copyright ScyllaDB, Inc. SPDX-License-Identifier: Apache-2.0
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {join} = require('node:path');
const {test} = require('node:test');
const {spawnSync} = require('node:child_process');
const yaml = require('js-yaml');
const guard = require('../scripts/release-notes-guard.cjs');

const repo = {owner: 'scylladb', repo: 'kafka-connect-scylladb'};
const label = 'omit-from-release-notes';
const file = (filename, extra = {}) => ({filename, status: 'modified', ...extra});
const workflowText = readFileSync(join(__dirname, '../workflows/label-release-notes.yml'), 'utf8');
const workflow = yaml.load(workflowText);
const steps = workflow.jobs.classify.steps;
const targetStep = steps.find(step => step.id === 'target');
const snapshotStep = steps.find(step => step.id === 'guard');
const finalizerStep = steps.at(-1);
const scripts = steps.filter(step => step.with?.script).map(step => step.with.script);
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
const trustedContext = {repo, eventName: 'pull_request_target', payload: {pull_request: {number: 275}}};
const manualContext = {repo, eventName: 'workflow_dispatch', payload: {}};

function fixture(options = {}) {
  const files = options.files ?? [file('README.md')];
  const labels = new Set(options.labels ?? ['unrelated']);
  const calls = [];
  let gets = 0;
  let listings = 0;
  const initial = {number: 275, head: {sha: 'head'}, base: {sha: 'base'}, changed_files: files.length};
  const github = {rest: {pulls: {
    get: async params => {
      assert.deepEqual(params, {...repo, pull_number: 275});
      calls.push('get');
      gets++;
      if (options.issueOnly) throw {status: 404, message: 'Not a pull request'};
      if (options.failGetAt === gets) throw new Error('PR lookup unavailable');
      return {data: {...initial, ...options.revisions?.[gets - 1]}};
    },
    listFiles: () => {}
  }, issues: {
    listLabelsOnIssue: () => {},
    removeLabel: async params => {
      assert.deepEqual(params, {...repo, issue_number: 275, name: label});
      calls.push('remove');
      if (options.cleanupFailure) throw new Error('Removal unavailable');
      const existing = [...labels].find(name => typeof name === 'string' && name.toLowerCase() === label);
      if (existing === undefined) throw {status: 404};
      labels.delete(existing);
    }
  }}, paginate: async (method, params) => {
    assert.equal(params.per_page, 100);
    if (method === github.rest.pulls.listFiles) {
      assert.deepEqual(params, {...repo, pull_number: 275, per_page: 100});
      calls.push('files');
      if (options.fileFailure) throw new Error('File listing unavailable');
      return options.listings?.[listings++] ?? files;
    }
    assert.equal(method, github.rest.issues.listLabelsOnIssue);
    assert.deepEqual(params, {...repo, issue_number: 275, per_page: 100});
    calls.push('labels');
    if (options.labelFailure) throw new Error('Labels unavailable');
    return [...labels].map(name => ({name}));
  }};
  return {github, labels, calls};
}

async function confirm(f, overrides = {}, context = trustedContext) {
  const outputs = {};
  const run = new AsyncFunction('github', 'context', 'core', 'require', 'process', targetStep.with.script);
  let error;
  try {
    await run(f.github, context, {setOutput: (key, value) => { outputs[key] = value; }},
      () => { throw new Error('Confirmation must not load helper code'); },
      {env: {PR_NUMBER: '275', ...overrides}});
  } catch (caught) { error = caught; }
  return {outputs, error, outcome: error ? 'failure' : 'success'};
}

async function finalize(f, hash, target, overrides = {}, load = () => guard) {
  const env = {PR_NUMBER: '275', CONFIRMED_PR: target.outputs['confirmed-pr'],
    TARGET_OUTCOME: target.outcome, CHECKOUT_OUTCOME: 'success', INSTALL_OUTCOME: 'success',
    GUARD_OUTCOME: 'success', LABELER_OUTCOME: 'success', UNSAFE_RENAME: 'false',
    SNAPSHOT_HASH: hash, ...overrides};
  const logs = [];
  const run = new AsyncFunction('github', 'context', 'core', 'require', 'process', finalizerStep.with.script);
  let error;
  try {
    await run(f.github, {repo}, {info: text => logs.push(text), error: text => logs.push(text)},
      load, {env});
  } catch (caught) { error = caught; }
  return {error, logs};
}

async function adapter(f, hash, overrides = {}, load = () => guard, context = trustedContext) {
  const target = await confirm(f, overrides, context);
  return {...await finalize(f, hash, target, overrides, load), target};
}

test('canonical policy matches mixed support files and keeps runtime or unknown paths visible', async () => {
  const files = ['.github/workflows/release.yml', '.github/.hidden',
    'src/test/java/io/connect/scylladb/Test.java', 'src/test/docker/docker-compose.yml',
    'images/guide.png', 'images/guide.svg', 'documentation/QUICKSTART.md', 'README.md',
    '.gitignore', 'renovate.json', 'scripts/release/test_guard.py',
    'scripts/release/test_workflow.py', 'scripts/release/test_github_release.py'].map(name => file(name));
  assert.equal((await guard.snapshot(fixture({files}).github, repo, 275)).omit, true);
  for (const name of ['pom.xml', 'src/main/java/io/connect/scylladb/Connector.java',
    'src/main/resources/scylla.json', 'scripts/release/github_release.py',
    'scripts/release/guard.py', 'scripts/tool.sh', 'examples/main.java',
    'docker-compose.yml', 'unknown.txt', 'documentation/nested/guide.md',
    'documentation/example.sh', 'images/nested/guide.png']) {
    const current = await guard.snapshot(fixture({files: [...files, file(name)]}).github, repo, 275);
    assert.equal(current.omit, false, name);
  }
});

test('renames use the actual canonical policy for both prior and current paths', async () => {
  for (const [previous, omit] of [['documentation/old.md', true], ['src/main/java/Connector.java', false], ['unknown', false]]) {
    const current = await guard.snapshot(fixture({files: [file('documentation/new.md', {
      status: 'renamed', previous_filename: previous})]}).github, repo, 275);
    assert.equal(current.omit, omit);
    assert.equal(current.unsafeRename, !omit);
  }
});

test('unsupported config shapes fail instead of approximating a second policy', () => {
  for (const source of ['{}', 'omit-from-release-notes: []',
    'omit-from-release-notes: [{head-branch: [main]}]',
    'omit-from-release-notes: [{changed-files: [{any-glob-to-any-file: ["**"]}]}]',
    'omit-from-release-notes: [{changed-files: [{all-globs-to-all-files: []}]}]',
    'other-label: [{changed-files: [{all-globs-to-all-files: ["**"]}]}]']) {
    assert.throws(() => guard.policy(source), /Unsupported labeler configuration/);
  }
});

test('PR input rejects malformed, ambiguous and unsafe numbers', () => {
  assert.equal(guard.parseNumber('275'), 275);
  for (const value of ['', '0', '-1', '01', '1.0', '1e2', ' 275', '275\n',
    '275,21', '${{ arbitrary }}', '9007199254740992', undefined, 275]) {
    assert.throws(() => guard.parseNumber(value), /canonical positive safe PR number/);
  }
});

test('manual confirmation uses the matching PR endpoint and trusted events need no lookup', async () => {
  const manual = fixture();
  const target = await confirm(manual, {}, manualContext);
  assert.ifError(target.error);
  assert.deepEqual(target.outputs, {'confirmed-pr': 'true'});
  assert.deepEqual(manual.calls, ['get']);
  const trusted = fixture({failGetAt: 1});
  assert.deepEqual((await confirm(trusted)).outputs, {'confirmed-pr': 'true'});
  assert.deepEqual(trusted.calls, []);
  const mismatch = fixture({issueOnly: true});
  const result = await confirm(mismatch, {}, {...trustedContext, payload: {pull_request: {number: 276}}});
  assert.ok(result.error);
  assert.deepEqual(result.outputs, {});
  assert.deepEqual(mismatch.calls, ['get']);
});

test('ordinary issues and unavailable or mismatched manual PR identities receive no label writes', async () => {
  for (const options of [{issueOnly: true}, {failGetAt: 1},
    ...[276, '275', undefined, null].map(number => ({revisions: [{number}]}))]) {
    const f = fixture({...options, labels: [label, 'unrelated']});
    const {error, target} = await adapter(f, '', {CHECKOUT_OUTCOME: 'skipped'},
      () => { throw new Error('Must not load helper'); }, manualContext);
    assert.ok(target.error);
    assert.deepEqual(target.outputs, {});
    assert.match(error.message, /not confirmed as a pull request/);
    assert.deepEqual(f.calls, ['get']);
    assert.deepEqual([...f.labels], [label, 'unrelated']);
  }
});

test('invalid target numbers and finalizers without confirmation receive no label writes', async () => {
  for (const value of ['', '0', '-1', '01', '1.0', '1e2', ' 275', '275\n',
    '275,21', '${{ arbitrary }}', '9007199254740992', undefined]) {
    const f = fixture({labels: [label]});
    const {error, target} = await adapter(f, '', {PR_NUMBER: value}, undefined, manualContext);
    assert.match(target.error.message, /canonical positive safe PR number/);
    assert.match(error.message, /canonical positive safe PR number/);
    assert.deepEqual(target.outputs, {});
    assert.deepEqual(f.calls, []);
  }
  const f = fixture({labels: [label]});
  const result = await finalize(f, '', {outputs: {}, outcome: 'skipped'}, {CHECKOUT_OUTCOME: 'skipped'});
  assert.match(result.error.message, /not confirmed as a pull request/);
  assert.deepEqual(f.calls, []);
});

test('manual confirmation survives a later lookup failure or missing helper', async () => {
  for (const [load, calls] of [
    [() => guard, ['get', 'get', 'remove']],
    [() => { throw new Error('Helper missing'); }, ['get', 'remove']]
  ]) {
    const f = fixture({labels: [label, 'unrelated'], failGetAt: 2});
    const {error, target} = await adapter(f, '', {}, load, manualContext);
    assert.ifError(target.error);
    assert.match(error.message, /lookup unavailable|Helper missing/);
    assert.deepEqual([...f.labels], ['unrelated']);
    assert.deepEqual(f.calls, calls);
  }
});

test('pagination requests full metadata and accepts a complete listing beyond one page', async () => {
  const f = fixture({files: Array.from({length: 105}, (_, i) => file(`documentation/${i}.md`))});
  const snapshot = await guard.snapshot(f.github, repo, 275);
  assert.equal(snapshot.count, 105);
  assert.equal(snapshot.omit, true);
  assert.match(snapshot.hash, /^[0-9a-f]{64}$/);
  assert.deepEqual(f.calls, ['get', 'files', 'get']);
});

test('zero, incomplete, truncated, duplicate and invalid file evidence fail visibly', async () => {
  const cases = [
    {files: []},
    {revisions: [{changed_files: 2}, {changed_files: 2}]},
    {files: [file('README.md'), file('README.md')]},
    {files: [file('README.md', {status: 'renamed'})]},
    {files: [file('README.md', {status: undefined})]},
    {files: [file('README.md', {previous_filename: ''})]},
    {files: [file('README.md')], listings: [[]]}
  ];
  for (const options of cases) await assert.rejects(guard.snapshot(fixture(options).github, repo, 275));
});

test('head, base and count changes during collection are rejected', async () => {
  for (const changed of [{head: {sha: 'new'}}, {base: {sha: 'new'}}, {changed_files: 2}]) {
    const f = fixture({revisions: [{}, changed]});
    await assert.rejects(guard.snapshot(f.github, repo, 275), /revisions changed/);
  }
});

test('verification detects before/after revisions and filename, status or prior-path changes', async () => {
  const original = await guard.snapshot(fixture().github, repo, 275);
  for (const options of [
    {revisions: [{head: {sha: 'new'}}, {head: {sha: 'new'}}]},
    {revisions: [{base: {sha: 'new'}}, {base: {sha: 'new'}}]},
    {files: [file('documentation/new.md')]},
    {files: [file('README.md', {status: 'added'})]},
    {files: [file('README.md', {status: 'renamed', previous_filename: 'documentation/old.md'})]}
  ]) {
    await assert.rejects(guard.verify(fixture(options).github, repo, 275, original.hash), /snapshot changed/);
  }
  const f = fixture({labels: [label], revisions: [{}, {}, {head: {sha: 'new'}}]});
  await assert.rejects(guard.verify(f.github, repo, 275, original.hash), /revisions changed during label/);
});

test('lookup, listing and actual-label lookup failures propagate', async () => {
  await assert.rejects(guard.snapshot(fixture({failGetAt: 1}).github, repo, 275), /lookup unavailable/);
  await assert.rejects(guard.snapshot(fixture({failGetAt: 2}).github, repo, 275), /lookup unavailable/);
  await assert.rejects(guard.snapshot(fixture({fileFailure: true}).github, repo, 275), /listing unavailable/);
  const original = await guard.snapshot(fixture().github, repo, 275);
  await assert.rejects(guard.verify(fixture({labelFailure: true}).github, repo, 275, original.hash), /Labels unavailable/);
});

test('actual-label verification catches stale state after internal action skip', async () => {
  for (const [files, labels] of [[[file('README.md')], []], [[file('src/main/java/Connector.java')], [label]]]) {
    const f = fixture({files, labels: [...labels, 'unrelated']});
    const original = await guard.snapshot(f.github, repo, 275);
    const {error} = await adapter(f, original.hash);
    assert.match(error.message, /Actual omission label/);
    assert.equal(f.labels.has(label), false);
    assert.equal(f.labels.has('unrelated'), true);
    assert.equal(f.calls.filter(call => call === 'remove').length, 1);
  }
});

test('failed final snapshot, lookup or label retrieval attempts cleanup', async () => {
  const original = await guard.snapshot(fixture().github, repo, 275);
  for (const options of [{files: [file('documentation/changed.md')]}, {failGetAt: 1},
    {fileFailure: true}, {labelFailure: true}]) {
    const f = fixture({...options, labels: [label, 'unrelated']});
    const {error} = await adapter(f, original.hash);
    assert.ok(error);
    assert.deepEqual([...f.labels], ['unrelated']);
    assert.equal(f.calls.filter(call => call === 'remove').length, 1);
  }
});

test('successful verification checks actual labels and preserves unrelated labels', async () => {
  for (const [files, labels] of [[[file('README.md')], [label]], [[file('src/main/java/Connector.java')], []]]) {
    const f = fixture({files, labels: [...labels, 'unrelated']});
    const original = await guard.snapshot(f.github, repo, 275);
    const {error, logs} = await adapter(f, original.hash);
    assert.ifError(error);
    assert.match(logs[0], /Verified head=head base=base count=1 snapshot=.*actual label checked/);
    assert.equal(f.labels.has('unrelated'), true);
    assert.ok(!f.calls.includes('remove'));
  }
});

test('actual labels use whole-name case-insensitive identity in both policy branches', async () => {
  for (const [filename, omitted] of [['README.md', true], ['src/main/java/Connector.java', false]]) {
    const f = fixture({files: [file(filename)], labels: ['Omit-From-Release-Notes', 'unrelated']});
    const original = await guard.snapshot(f.github, repo, 275);
    const {error} = await adapter(f, original.hash);
    if (omitted) {
      assert.ifError(error);
      assert.equal(f.labels.has('Omit-From-Release-Notes'), true);
      assert.ok(!f.calls.includes('remove'));
    } else {
      assert.match(error.message, /Actual omission label/);
      assert.equal(f.labels.has('Omit-From-Release-Notes'), false);
      assert.equal(f.calls.filter(call => call === 'remove').length, 1);
    }
    assert.equal(f.labels.has('unrelated'), true);
  }
  const f = fixture({files: [file('src/main/java/Connector.java')],
    labels: ['prefix-omit-from-release-notes', 'omit-from-release-notes-extra']});
  const original = await guard.snapshot(f.github, repo, 275);
  assert.ifError((await adapter(f, original.hash)).error);
  assert.ok(!f.calls.includes('remove'));
});

test('malformed actual-label metadata rejects verification and attempts cleanup', async () => {
  const f = fixture({files: [file('src/main/java/Connector.java')], labels: [null, 'unrelated']});
  const original = await guard.snapshot(f.github, repo, 275);
  const {error} = await adapter(f, original.hash);
  assert.match(error.message, /Invalid actual-label metadata/);
  assert.equal(f.calls.filter(call => call === 'remove').length, 1);
  assert.equal(f.labels.has('unrelated'), true);
});

test('intentional unsafe-rename skip removes omission label and verifies visible state', async () => {
  const f = fixture({files: [file('documentation/new.md', {status: 'renamed', previous_filename: 'src/main/java/Connector.java'})],
    labels: [label, 'unrelated']});
  const original = await guard.snapshot(f.github, repo, 275);
  const {error, logs} = await adapter(f, original.hash, {UNSAFE_RENAME: 'true', LABELER_OUTCOME: 'skipped'});
  assert.ifError(error);
  assert.match(logs[0], /omit=false; actual label checked/);
  assert.deepEqual([...f.labels], ['unrelated']);
  assert.equal(f.calls.filter(call => call === 'remove').length, 1);
});

test('confirmed PRs retain cleanup after failed or skipped setup, guard and labeler without the helper', async () => {
  for (const context of [trustedContext, manualContext]) {
    for (const key of ['TARGET_OUTCOME', 'CHECKOUT_OUTCOME', 'INSTALL_OUTCOME', 'GUARD_OUTCOME', 'LABELER_OUTCOME']) {
      for (const outcome of ['failure', 'skipped', 'cancelled', '']) {
        const f = fixture({labels: [label, 'unrelated']});
        const {error} = await adapter(f, '', {[key]: outcome}, () => { throw new Error('Must not require helper'); }, context);
        assert.match(error.message, /predecessor failed or unexpectedly skipped/);
        assert.deepEqual([...f.labels], ['unrelated']);
        assert.deepEqual(f.calls, context === manualContext ? ['get', 'remove'] : ['remove']);
      }
    }
  }
  const f = fixture({labels: [label]});
  const {error} = await adapter(f, '', {}, () => { throw new Error('Helper missing'); });
  assert.match(error.message, /Helper missing/);
  assert.deepEqual(f.calls, ['remove']);
});

test('cleanup API failures remain visible without losing the original failure or retrying', async () => {
  const f = fixture({cleanupFailure: true});
  const result = await adapter(f, '', {INSTALL_OUTCOME: 'failure'});
  assert.match(result.error.message, /predecessor failed/);
  assert.match(result.logs[0], /cleanup failed: Removal unavailable/);
  assert.deepEqual(f.calls, ['remove']);
  const unsafe = fixture({cleanupFailure: true});
  const result2 = await adapter(unsafe, '', {UNSAFE_RENAME: 'true', LABELER_OUTCOME: 'skipped'});
  assert.match(result2.error.message, /Removal unavailable/);
  assert.deepEqual(unsafe.calls, ['remove']);
});

test('workflow adapters use environment input and emit bounded verified snapshot outputs', async () => {
  const run = new AsyncFunction('github', 'context', 'core', 'require', 'process', snapshotStep.with.script);
  const outputs = {};
  const f = fixture();
  await run(f.github, {repo}, {setOutput: (key, value) => { outputs[key] = value; }, info: () => {}},
    () => guard, {env: {PR_NUMBER: '275'}});
  assert.deepEqual(Object.keys(outputs), ['hash', 'unsafe-rename']);
  assert.equal(outputs['unsafe-rename'], 'false');
  const invalid = await adapter(f, '', {PR_NUMBER: '01'});
  assert.match(invalid.error.message, /canonical positive safe PR number/);
  assert.equal(steps.at(-1).if, 'always()');
  assert.equal(finalizerStep.env.CONFIRMED_PR, '${{ steps.target.outputs.confirmed-pr }}');
  assert.equal(finalizerStep.env.TARGET_OUTCOME, '${{ steps.target.outcome }}');
  assert.equal(targetStep.if, undefined);
  assert.ok(steps.indexOf(targetStep) < steps.findIndex(step => step.id === 'checkout'));
  assert.ok(steps.indexOf(targetStep) < steps.findIndex(step => step.id === 'install'));
  assert.equal(workflow.on.workflow_dispatch.inputs['pull-request'].required, true);
  assert.equal(workflow.jobs.classify.env.PR_NUMBER, '${{ github.event.pull_request.number || inputs.pull-request }}');
  assert.deepEqual(workflow.permissions, {});
  assert.deepEqual(workflow.jobs.classify.permissions, {contents: 'read', 'pull-requests': 'write', issues: 'write'});
  const checkout = steps.find(step => step.id === 'checkout');
  assert.equal(checkout.with.ref, '${{ github.sha }}');
  assert.equal(checkout.with['persist-credentials'], false);
  assert.equal(steps.find(step => step.id === 'install').run, 'npm ci --ignore-scripts --prefix .github');
  const action = steps.find(step => step.id === 'labeler');
  assert.equal(action.if, "success() && steps.guard.outputs.unsafe-rename == 'false'");
  assert.equal(action.with['sync-labels'], true);
  assert.equal(action.with['pr-number'], '${{ env.PR_NUMBER }}');
  for (const script of scripts) assert.ok(!script.includes('${{'));
});

test('manual backfill rejects nondefault refs before confirmation or loading helper code', async () => {
  const step = steps.find(step => step.id === 'dispatch-ref');
  assert.equal(step.if, "github.event_name == 'workflow_dispatch'");
  assert.equal(step.env.DISPATCH_REF, '${{ github.ref }}');
  assert.equal(step.env.DEFAULT_BRANCH, '${{ github.event.repository.default_branch }}');
  assert.ok(steps.indexOf(step) < steps.indexOf(targetStep));
  assert.ok(!step.run.includes('${{'));
  for (const [branch, ref, success] of [
    ['master', 'refs/heads/master', true], ['main', 'refs/heads/main', true],
    ['master', 'refs/heads/topic', false], ['master', 'refs/tags/master', false],
    ['master', '', false], ['', 'refs/heads/', false]
  ]) {
    const result = spawnSync('bash', ['-euo', 'pipefail', '-c', step.run], {
      env: {...process.env, DEFAULT_BRANCH: branch, DISPATCH_REF: ref},
      encoding: 'utf8', timeout: 5000
    });
    assert.ifError(result.error);
    assert.equal(result.status === 0, success, `${branch}: ${ref}`);
    if (!success) {
      assert.match(result.stdout, /::error::.*default branch/);
      const f = fixture({labels: [label]});
      const final = await finalize(f, '', {outputs: {}, outcome: 'skipped'}, {CHECKOUT_OUTCOME: 'skipped'});
      assert.match(final.error.message, /not confirmed as a pull request/);
      assert.deepEqual(f.calls, []);
    }
  }
});
