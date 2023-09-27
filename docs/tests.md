Tests
================

When running tests, all should run ok without problems:


```
python -m unittest
..................................................................
----------------------------------------------------------------------
Ran 66 tests in 0.097s

OK
```
<br/>

If one test fail it will appear like this:

```
python -m unittest
......................F...........................................
======================================================================
FAIL: test_save_raw_dataframe (tests.station_set.test_extract_methods.ExtractMethodsTestCase.test_save_raw_dataframe)
----------------------------------------------------------------------
Traceback (most recent call last):
  File "/arbol/nettle/tests/station_set/test_extract_methods.py", line 29, in test_save_raw_dataframe
    self.assertTrue(False)
AssertionError: False is not true

----------------------------------------------------------------------
Ran 66 tests in 0.074s

FAILED (failures=1)
```

### Coverage:

Generate coverage:

```
python -m coverage run -m unittest
..................................................................
----------------------------------------------------------------------
Ran 66 tests in 0.122s

OK
```
<br/>

Generate coverage report in CLI:

```
python -m coverage report
Name                                  Stmts   Miss  Cover
---------------------------------------------------------
nettle/dataframe/validators.py           19      0   100%
nettle/errors/custom_errors.py            6      0   100%
nettle/io/file_handler.py                24      2    92%
nettle/io/ipfs.py                        71     48    32%
nettle/io/store.py                      263    150    43%
nettle/metadata/bases.py                  2      0   100%
nettle/metadata/metadata_handler.py      53      2    96%
nettle/metadata/validators.py            11      1    91%
nettle/station_set.py                   247     94    62%
nettle/utils/date_range_handler.py       39      3    92%
nettle/utils/log_info.py                 25      7    72%
nettle/utils/settings.py                 10      0   100%
---------------------------------------------------------
TOTAL                                   770    307    60%
```
<br/>

Generate HTML report:
`python -m coverage html`

![image](https://github.com/Arbol-Project/nettle/assets/11861161/99274c79-b4b2-41db-86eb-bedb6a075b61)