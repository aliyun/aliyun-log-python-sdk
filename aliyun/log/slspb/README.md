## install

```
python3 setup.py install
```

## use

```
import slspb

# parse __time__ as str
slspb.parse_pb(rawpbcontent,1)

# do not parse __time__ as str
slspb.parse_pb(rawpbcontent,0)
```
