# CHANGELOG

## v0.3.0 (2024-07-07)

### Chore

* chore: update requirements ([`8227fa7`](https://github.com/Sabmus/latam-de-challenge/commit/8227fa7209d95cd09a528f1f40409ebdbb2b9f1a))

### Documentation

* docs: add docs ([`ff9b308`](https://github.com/Sabmus/latam-de-challenge/commit/ff9b308bb692a7b66532e82a8af0785f8010b5e2))

* docs: update jupyter notebooks ([`40dcfaf`](https://github.com/Sabmus/latam-de-challenge/commit/40dcfaf2abc76f02dfffb5bb91c68c26ac2570ec))

### Feature

* feat: add pandas implementation ([`53573c3`](https://github.com/Sabmus/latam-de-challenge/commit/53573c3bec5ec2584d94eee33fc28e512b8e2420))

* feat: add implementation for q2 time ([`744dd03`](https://github.com/Sabmus/latam-de-challenge/commit/744dd0318c520402a42ab85a66119477944beb8a))

* feat: add implementation for q2 memory ([`54776a9`](https://github.com/Sabmus/latam-de-challenge/commit/54776a91860c66c3426fbf8d4e4a6a5d8feb471b))

### Fix

* fix: typos ([`c93faf9`](https://github.com/Sabmus/latam-de-challenge/commit/c93faf98b2e1c50c7602a99dc64673c5e0b2da9d))

* fix: change stop spark method ([`890c735`](https://github.com/Sabmus/latam-de-challenge/commit/890c735c1fab8f8ef5301496bcfd46738a3f358f))

* fix: change spark app name ([`46c8f6a`](https://github.com/Sabmus/latam-de-challenge/commit/46c8f6a4ba3432c428d3c3417ddcf43f36d12131))

### Performance

* perf: same as q2-time ([`3994c6c`](https://github.com/Sabmus/latam-de-challenge/commit/3994c6c47476221b67b1004eca9ef8f6128c5a34))

* perf: improve performance by reducing content column size

with short_pattern content column was reduced eliminating almost every non emoji character ([`8705e19`](https://github.com/Sabmus/latam-de-challenge/commit/8705e194ef950e1e741db629814dd1431474d3d3))

* perf: add json to parquet conversion

in order to improve memory and time, I create a json to parquet transformation.

With this, I manage to reduce the total file size from 389mb to 38mb (~90%) ([`bcca793`](https://github.com/Sabmus/latam-de-challenge/commit/bcca793b38d795dcc3c72d4a8bce247b24ecd7bb))

### Refactor

* refactor: remove id column ([`f82a95e`](https://github.com/Sabmus/latam-de-challenge/commit/f82a95e424899efa53e819b6ccf6ac9297adbef6))

* refactor: add repartition on date_only column ([`9595c16`](https://github.com/Sabmus/latam-de-challenge/commit/9595c1623c064f9bd8da73ee92f8dc07d95dc0ae))

* refactor: add implementation using pandas ([`be58cf4`](https://github.com/Sabmus/latam-de-challenge/commit/be58cf4472840b9988a0c76c9c6a8a7af6424ebc))

* refactor: add cache, and change limit/collect for take ([`24bac46`](https://github.com/Sabmus/latam-de-challenge/commit/24bac46cbdd7cb1726b3fdf73c511aeca98854b0))

* refactor: improve memory using cache ([`1e5458d`](https://github.com/Sabmus/latam-de-challenge/commit/1e5458d3e1ed7f94133034ced1ec4e814ba127ab))

* refactor: add implementation using pandas ([`1c82fa2`](https://github.com/Sabmus/latam-de-challenge/commit/1c82fa26698743a9adce1db239d53624e8d8b99f))

* refactor: add comments and reduce lines of code ([`a7fd83a`](https://github.com/Sabmus/latam-de-challenge/commit/a7fd83a1f27dc13b1763191b350c90f7bfc308fa))

* refactor: add comments ([`12a5815`](https://github.com/Sabmus/latam-de-challenge/commit/12a5815154cc59f45340680bbd392f1a661a12d8))

* refactor: create variable names for configuration fields ([`71bd91e`](https://github.com/Sabmus/latam-de-challenge/commit/71bd91e8cd9a79e903fc799044da92a69374ab0c))

* refactor: remove comments, and cache ([`6746f27`](https://github.com/Sabmus/latam-de-challenge/commit/6746f27516428d234b10c124e5cb3d2619e98fdb))

* refactor: use parquet file instead of json ([`6ec46c2`](https://github.com/Sabmus/latam-de-challenge/commit/6ec46c26368905b820026c22683b748d6e69c0fc))

* refactor: remove unused references ([`6742c06`](https://github.com/Sabmus/latam-de-challenge/commit/6742c067aac692259e0e1c16f30b40e07a601300))

* refactor: simplify logic for extracting quotedTweets

I create a simplier method for extracting quotedTweets.

Since Parquet is going to be used, I only need to have all the tweets in one parquet file, to then just read it and select the columns that I need ([`d5b3cb9`](https://github.com/Sabmus/latam-de-challenge/commit/d5b3cb95a9a2aebbba4fb3f8cd8204c3585c6281))

* refactor: change to improve performance

try to improve perfomance by reducing the length of emojis pattern ([`31c7a26`](https://github.com/Sabmus/latam-de-challenge/commit/31c7a26e4c6c9598cb5e19b10f8d521278c26e0c))

* refactor: add cores config for spark ([`f54807f`](https://github.com/Sabmus/latam-de-challenge/commit/f54807f5a132d08362520454eb42c26ba087442c))

* refactor: reduce df column quantity ([`3c86072`](https://github.com/Sabmus/latam-de-challenge/commit/3c860725c6400212bc9d21dbb6ddd70401def5ca))

* refactor: comment code to test q2 ([`7bd565a`](https://github.com/Sabmus/latam-de-challenge/commit/7bd565a816cb73c69b15ab482ff61d5d9b1e9556))

* refactor: change variable names and add stats dump ([`ca0f55c`](https://github.com/Sabmus/latam-de-challenge/commit/ca0f55c43949c739ce77791552a516f024e17b73))

### Style

* style: change mardown to html for styling ([`b174986`](https://github.com/Sabmus/latam-de-challenge/commit/b1749869de5ef5d784c4ded1ee63579f712aab98))

### Test

* test: add test for all functions ([`ed84ff2`](https://github.com/Sabmus/latam-de-challenge/commit/ed84ff2b69f0ed28f87df6887cef90986a1fcf86))

* test: add test for Q1, Q2, Q3 - time ([`3bc012d`](https://github.com/Sabmus/latam-de-challenge/commit/3bc012d5ecdc47ff0dbf9f270663f4c65f2f8302))

### Unknown

* Merge pull request #24 from Sabmus/release

Final release ([`ee353aa`](https://github.com/Sabmus/latam-de-challenge/commit/ee353aa27fb04f878a113ccdc63af5e1162852b5))

* other: update time stat files ([`df68b33`](https://github.com/Sabmus/latam-de-challenge/commit/df68b330bffa4af0d39c20ebf98e1a8a505fb4c2))

* other: add docs in comment ([`cedd779`](https://github.com/Sabmus/latam-de-challenge/commit/cedd77974720f8e2ce1d7d12cd31fc6b4cc9bb74))

* other: remove blank line ([`b417888`](https://github.com/Sabmus/latam-de-challenge/commit/b4178889665a174d56223d99d70705f1a2468abb))

* Merge pull request #23 from Sabmus/feature/q3-time

refactor: add implementation using pandas ([`3610d56`](https://github.com/Sabmus/latam-de-challenge/commit/3610d5618fb3bd86b70174e10505644667d1c987))

* Merge pull request #22 from Sabmus/feature/q3-memory

refactor: add cache, and change limit/collect for take ([`65497b9`](https://github.com/Sabmus/latam-de-challenge/commit/65497b9046cc4ecb2c8ab19715e3fb41cf3e1f52))

* other: add comments ([`a3a15f8`](https://github.com/Sabmus/latam-de-challenge/commit/a3a15f82872e3806d9eee22c72da473b155a7df1))

* Merge pull request #21 from Sabmus/feature/q1-memory

refactor: improve memory using cache ([`ee45832`](https://github.com/Sabmus/latam-de-challenge/commit/ee45832bbe82fc77f934392825d99b15e7defeef))

* Merge pull request #20 from Sabmus/feature/q1-time

refactor: add implementation using pandas ([`a7a2edb`](https://github.com/Sabmus/latam-de-challenge/commit/a7a2edb5fca9979b0ea928d958f1ee5f0203f122))

* Merge pull request #19 from Sabmus/feature/q2-time

Feature/q2 time ([`49043c3`](https://github.com/Sabmus/latam-de-challenge/commit/49043c3749d80ef19d3c767f3fd8ada37927de35))

* other: add time_perf file for py-spy ([`2da8d61`](https://github.com/Sabmus/latam-de-challenge/commit/2da8d619fedead6f6a8b559d21fc2eb9ae39a78f))

* Merge pull request #18 from Sabmus/feature/q2-memory

perf: same as q2-time ([`5332618`](https://github.com/Sabmus/latam-de-challenge/commit/5332618cf453aada81656ad56afb5c5cc3ebec77))

* Merge pull request #17 from Sabmus/feature/q2-time

Feature/q2 time ([`4e27abc`](https://github.com/Sabmus/latam-de-challenge/commit/4e27abc489716777d1327d1533951c3a18a40aa9))

* other: update after notebook run ([`71190d8`](https://github.com/Sabmus/latam-de-challenge/commit/71190d87ade3c142c37ba9c6a519b010dbf30bc4))

* other: update time stats files ([`1a32c6f`](https://github.com/Sabmus/latam-de-challenge/commit/1a32c6fbb33b12275f9fd3b35cba2dd1f80f6073))

* Merge pull request #16 from Sabmus/feature/q2-time

feat: add implementation for q2 time ([`78edcb2`](https://github.com/Sabmus/latam-de-challenge/commit/78edcb2206f65a8d620520e23287aaee420af30b))

* Merge pull request #15 from Sabmus/feature/q2-memory

Feature/q2 memory ([`6bbbd8c`](https://github.com/Sabmus/latam-de-challenge/commit/6bbbd8cc8cbb2f5e3ff53b7aa2b20b689a9816d3))

* Merge branch &#39;dev&#39; into feature/q2-memory ([`5c15e0e`](https://github.com/Sabmus/latam-de-challenge/commit/5c15e0edee9d11d8699b8bd136cdeebfa3e593f4))

## v0.2.0 (2024-07-04)

### Feature

* feat: add implementation for q3 memory ([`b54eb4a`](https://github.com/Sabmus/latam-de-challenge/commit/b54eb4aa9a6d75a99614c2dfe15b9aa2bf90b970))

### Fix

* fix: remove duplicates by id ([`9d1ceea`](https://github.com/Sabmus/latam-de-challenge/commit/9d1ceea57174e6acbfce310474bd800d58cdd37f))

### Refactor

* refactor: remove tags from on push actions ([`1d6495c`](https://github.com/Sabmus/latam-de-challenge/commit/1d6495cbf7e8ee3fa31dd5c0d1570c4490f62f51))

* refactor: add common function to extract data from main dataset ([`3ddb6ea`](https://github.com/Sabmus/latam-de-challenge/commit/3ddb6ea48ad467fe51842950dc741a7dc71e990a))

* refactor: add partial implementation for q3 time ([`92f4e71`](https://github.com/Sabmus/latam-de-challenge/commit/92f4e7175683bd77122d4fa9a002de71b3772002))

* refactor: add partial implementation for q3_memory ([`1e41885`](https://github.com/Sabmus/latam-de-challenge/commit/1e418850fa41d4be89075caac7bb5554fb5b6a1d))

* refactor: add partial q2 memory implementation ([`1fb814f`](https://github.com/Sabmus/latam-de-challenge/commit/1fb814f594e0cb4d1bdfc3c59a5f3542cf8e8e49))

### Unknown

* Merge pull request #14 from Sabmus/release

Release Q2 ([`874f0ec`](https://github.com/Sabmus/latam-de-challenge/commit/874f0ec84f4e4f372a7e5593d90e31e878848bdd))

* Merge pull request #13 from Sabmus/feature/q3-time

Feature/q3 time ([`b81478a`](https://github.com/Sabmus/latam-de-challenge/commit/b81478a13a9fc73996fa4fc783049955c5d6446b))

* Merge pull request #12 from Sabmus/feature/q3-memory

Feature/q3 memory ([`e13632c`](https://github.com/Sabmus/latam-de-challenge/commit/e13632cd95d922c3543ba00409dc59a708a7f906))

## v0.1.0 (2024-07-03)

### Chore

* chore: add config files ([`a9d7691`](https://github.com/Sabmus/latam-de-challenge/commit/a9d76913ac0ed0a0fc9714c6ff3078cfbee9da0e))

* chore: add code for test q1_time speed ([`a47cad7`](https://github.com/Sabmus/latam-de-challenge/commit/a47cad75f1df6d847cc24d947fe2f95118866f3b))

* chore: add github actions for tags ([`ecef3ad`](https://github.com/Sabmus/latam-de-challenge/commit/ecef3ad0e76a137444cea3400b34d55e2c9ec5c3))

* chore: add __init__ ([`e3beb21`](https://github.com/Sabmus/latam-de-challenge/commit/e3beb21e5354aca56b6683ccbc03262a47860edf))

* chore: update gitignore ([`d2c97a5`](https://github.com/Sabmus/latam-de-challenge/commit/d2c97a5963cb471cff3dc3d03670805b8dd1e886))

* chore: update dependecies ([`c2e0838`](https://github.com/Sabmus/latam-de-challenge/commit/c2e083885669c819cef390296157380f60528009))

* chore: add gitignore ([`13ef82b`](https://github.com/Sabmus/latam-de-challenge/commit/13ef82b5e8cb7350a0105036a0ef84eadd23553e))

### Feature

* feat: add implementation for question 1 ([`5b7b6bb`](https://github.com/Sabmus/latam-de-challenge/commit/5b7b6bb8d58a3786f9ecef0d021cdb7cee8eafd9))

* feat: add implementation for q1_time ([`c5af77e`](https://github.com/Sabmus/latam-de-challenge/commit/c5af77edcb6613b5c1c3b898552985a898823ea3))

* feat: add implementation for q1_memory ([`ec20cdf`](https://github.com/Sabmus/latam-de-challenge/commit/ec20cdfd45dcf3564669dbb3defb816dc99aec2d))

* feat: add q1_memory initial implementation ([`978e11c`](https://github.com/Sabmus/latam-de-challenge/commit/978e11c190dd9f8203527ea1dea950bdf2a0eebd))

* feat: add SparkClass

For managing Spark sessions and loading JSON files ([`8bb7d0e`](https://github.com/Sabmus/latam-de-challenge/commit/8bb7d0ef889533f0def6b0e93306424eb42b433e))

### Fix

* fix: github actions config ([`cb7976e`](https://github.com/Sabmus/latam-de-challenge/commit/cb7976e8f619842ee7b9f425742abff59bab3485))

### Refactor

* refactor: change logic to extract nested quoted tweets

the idea was to create a function to automatically iterate over the main df to look for any tweets inside quotedTweets. with with now I have all the tweets for the dataset ([`1209ecc`](https://github.com/Sabmus/latam-de-challenge/commit/1209ecc77dabf10806f610553e77b9e78a1558ae))

* refactor: change pyspark functions import ([`6dc668a`](https://github.com/Sabmus/latam-de-challenge/commit/6dc668a462371a6544d700831c07880e4c7baa63))

### Unknown

* Merge pull request #11 from Sabmus/hotfix/github-workflow

Hotfix/GitHub workflow ([`5966623`](https://github.com/Sabmus/latam-de-challenge/commit/5966623af9d5c2443189a3c83ecc28de4f946a97))

* Merge pull request #9 from Sabmus/release

Release v0.1.0 ([`73c30cb`](https://github.com/Sabmus/latam-de-challenge/commit/73c30cb3fa419525c6a9f63f29fce56eee55e695))

* Merge branch &#39;feature/q1-memory&#39; into dev ([`b270433`](https://github.com/Sabmus/latam-de-challenge/commit/b27043391ea50be1d6ed5d4f3b491a62934d742f))

* other: add file check to ipynb

I added a file check to find the json file or the zip file ([`33f1934`](https://github.com/Sabmus/latam-de-challenge/commit/33f193496b31ba81feefda8f3a71a8babc26d7fe))

* Merge pull request #8 from Sabmus/feature/pyspark

Feature/pyspark ([`ac88d7c`](https://github.com/Sabmus/latam-de-challenge/commit/ac88d7c8e9a73280d21980cd893284dbeea085ee))

* first commit ([`10def92`](https://github.com/Sabmus/latam-de-challenge/commit/10def928f9dc6c7cfc8d610063cbd0ee7f2dad7e))
