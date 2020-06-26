# Designing a Dask DataFrame `.iloc` method
Implementing distributed `.iloc` requires mapping between two "index spaces":
- **idx** space:
  - linear idxs into the overall DDF
  - users pass these to `.iloc`
- **part-idx** ("partition index") space:
  - each row is associated with a `(block idx, intra-block idx)` tuple:
    - `block idx` is the index of the containing block (a.k.a. "partition") in the DDF's array of blocks (each of which is a `pd.DataFrame`)
    - `intra-block idx` is the index of the row within its block
  - note that both spaces support positive- and negative-integer indices, in principle

## Computing "part-idxs"
Computing **part-idxs** from **idxs** can be expensive; you have to know all block sizes "to the left" (resp. "right") of positive (resp. negative) part-idxs.

That typically requires `.compute()`ing some blocks, and then either:
1. throwing them away
  - when you needed to find a **part-idx** *from which to start returning elements*
  - e.g. in response to a slice like `[1000:]`
2. recomputing them from scratch once you've identified the **part-idxs** that are responsive to the given **idx**-range
  - e.g. in response to a slice like `[:1000]`

Each of these cases comes with a serious foot-gun:

### 1. Find a block to start from, take blocks from there (e.g. `.iloc[1000:]`) <a id="compute-suffix"></a>
Naively, a slice like `[1000:]` can be served by:
- `compute`ing (really just getting the `len()` of) blocks from the start until you've seen 1000 elements (more detail on how to do this [below](#iterative-block-group-computing))
- return a DDF that:
  - does an appropriate `.iloc` inside the block containing **idx** 1000 (so that **idx** 1000 is the first row in the returned block, and new DDF)
  - returns [that first `.iloc`'d/partial block] prepended to [all subsequent blocks (in full)]

#### Difficulty: existence of Nearest Shuffle-Stage Ancestor (NSA)  <a id="nsa-difficulty"></a>
Unfortunately, a dependency of the DDF being `.iloc`'d may be the result of an "all-to-all shuffle" (where each result block is potentially comprised of elements from any of the blocks on the input side; distributed sorts, group-by's, etc. generally have this property).

This means that getting just the first block of the DDF (to check its `len`) can require *all partitions* of (some or all of) its dependency graph to be computed

Additionally, if we find out that `[1000:]` is served by looking at blocks `[3:]`, any later `compute` that uses those `[3:]` blocks will again compute *all partitions* of some/all dependencies.

#### Mitigation: `persist()` the NSA <a id="persist-nsa"></a>
One solution is to find the nearest shuffle-stage ancestor and fully `compute` and [`.persist()`](https://distributed.dask.org/en/latest/memory.html#persisting-collections) it.

##### Caveats: memory pressure, spilling? <a id="persist-nsa-caveats"></a>
I'm not sure whether Dask spills to disk if `.persist`ing causes memory pressue:
- If it does, an `.iloc` that relies on this optimization can potentially be quite expensive
- If it doesn't, an `.iloc` is liable to OOM workers

So, even the best possible implementation here will likely still have some subtle but occasionally serious "gotchas".

### 2. Take all blocks up to a certain point (e.g. `.iloc[:1000]`) <a id="compute-prefix"></a>
The main danger (and mitigations) here are basically the same as above:
- Suppose you learn that **idx** `1000` is in block `3` (again, [see below for discussion about how](#iterative-block-group-computing))
- Great! you can pass along a new DDF made of just blocks `[:3]` (this time with the *last* block `.iloc`'d/partial, instead of the *first*)
- However, you still fully computed any upstream shuffle stages
  - you'll probably want to find the [NSA](#nsa-difficultry), `.persist()` it, and hope for the best (as in [case 1. above](#persist-nsa-caveats))
- Additionally, you probably computed all rows in blocks `[:3]` (to get the blocks' lengths), and then threw away the rows
  - you should probably `persist` blocks `[:3]` as well
    - but, if you persist blocks `[:3]`, do you still need to persist the full NSA?
      - I believe the answer is "yes".
      - The problem is how you learned that `3` blocks was enough to get the first `1000` rows.
      - In general, you'd have to either have:
        - computed all block sizes in the DDF being `.iloc`'d, or
        - embarked on an [iterative-widening "try computing the first K blocks and seeing how many elements that fetches" approach](#iterative-block-group-computing), which warrants `persist`ing any nearest shuffle ancestor (NSA)
    - so, you'll want to `persist` both the NSA as well as the blocks that contain the **idxs** you seek. 

### `len`-only pipeline mode?
There may be situations where it's easy+cheap to run the whole graph upstream of a DDF *while only computing the partition sizes at each stage*.

Can a final per-block `len()` be "pushed-down" so that a DDF could compute all its block-sizes (either at graph-construction time or `.iloc`-execution time), allowing arbitrary `.iloc`'ing and filtering of blocks to construct the result DDF?

Some examples where this optimization would be possible: 
- A DDF is loaded from blocks read from an HDF5 `Dataset`
- A DDF's blocks are constructed from a Dask Array of known chunk sizes 

In both cases, each DDF block's length will be known at graph-construction time.

If the next Dask op on such a DDF merely slices out some columns, that DDF still knows its block-sizes, and can be trivially `.iloc`'d. Block-`len`s can be propagated through some complex graphs without `.compute()`ing anything.

### Example Slices, required part-idxs <a id="part-idx-table"></a>

Here are a bunch of slices, and which "positive" (`pos`) and "negative" (`neg`) **idxs** we must compute **part-idxs** for in order to serve an `.iloc` of that slice:
<table>
  <tbody>
  <tr>
    <th colspan="2" rowspan="3"></th>
    <th colspan="9"><code>end</code></th>
  </tr>
  <tr>
    <th colspan="3"><code>+</code></th>
    <th colspan="3"><code>-</code></th>
    <th colspan="3"><code>:</code></th>
  </tr>
  <tr>
    <th>slice</th><th>pos</th><th>neg</th>
    <th>slice</th><th>pos</th><th>neg</th>
    <th>slice</th><th>pos</th><th>neg</th>
  </tr>
  <tr>
    <td rowspan="3"><b><code>start</code></b></td>
    <td><code><b>+</b></code></td>
    <td><code>[&nbsp;&nbsp;5:10]</code></td><td><code>10</code></td><td></td>
    <td><code>[&nbsp;10:&nbsp;-5]</code></td><td><code>10</code></td><td><code>-5</code></td>
    <td><code>[&nbsp;10:&nbsp;&nbsp;]</code></td><td><code>10</code></td><td></td>
  </tr>
  <tr>
    <td><code><b>-</b></code></td>
    <td><code>[-10:10]</code></td><td><code>10</code></td><td><code>-10</code></td>
    <td><code>[-10:&nbsp;-5]</code></td><td></td><td><code>-10</code></td>
    <td><code>[-10:&nbsp;&nbsp;]</code></td><td></td><td><code>-10</code></td>
  </tr>
  <tr>
    <td><code><b>:</b></code></td>
    <td><code>[&nbsp;&nbsp;&nbsp;:10]</code></td><td><code>10</code></td><td></td>
    <td><code>[&nbsp;&nbsp;&nbsp;:-10]</code></td><td></td><td><code>-10</code></td>
    <td><code>[&nbsp;&nbsp;&nbsp;:&nbsp;&nbsp;]</code></td><td></td><td></td>
  </tr>
  </tbody>
</table>

### Fetching groups of blocks *forward from the first block* or *backward from the last block* <a id="iterative-block-group-computing"></a>
All slices above require finding zero or one positive **part-idxs** and zero or one negative **part-idxs**.

Here's an algorithm for computing a **part-idx** starting from either end:

1. `.compute()` the first $N$ blocks from the appropriate end
  - $N=5$ can be our own hard-coded default / initial value
  - maybe there's a global to change it if you want
2. If those blocks contain the end of the **idx** we are seeking, we're done
  - some further partial-filtering of the block containing the **idx** should be handled separately
3. Otherwise:
  - Estimate avg records/block based on all blocks computed so far
  - Figure out how many more blocks are likely required to cover desired **idx** (and throw in some padding)
  - Go back to step 1. but with new $N$

I suspect the number of successive `compute()`s (repetitions of step 1.) will have a mode of 1 (and average of like, 1.2?). It should perform reasonably well and not be too wasteful, in most cases.

#### `raise` on slices that waste too many rows?

One possible "gotcha" we might like to protect against by default:

What if a user asks for `ddf.iloc[1e8:(1e8+10)]` (assume the DF is still bigger, like `1e9` rows, so we know we should treat the slice as a "prefix")?

Should we `compute` the first `1e8+10` rows just to serve them 10 rows?

If the user expects random access to those 10 rows, maybe it's correct to `raise`, because it's too dangerous to risk surprising them with such a huge computation. Maybe that's part of why Dask does `raise`s today.

##### Configurable "wasted rows" threshold
I can imagine picking a number, say 99%, and `raise`ing if the user requests a slice that will be >99% wasted rows. You can do `df.iloc[1e8:2e8]` but not `df.iloc[1e8:(1e8+1e4)]`, and the threshold can be a global/env/config var.

Building guardrails here may also not be necessary / worthwhile.
