# PyMetaGen

The objective of this library is to generate a useful metadata from a given
data file type:
    - **PARQUET**
    - **JSON**
    - **CSV**

Focus mainly on `parquet` files.

| **Name** | **Type** | **Min** | **Max** | **Min Length** | **Max Length** | **# nulls** | **# empty/zero** | **# positive** | **# negative** | **Any duplicates** | **Definition**|
|:-----|:-----|:----|:----|-----------:|-----------:|-------:|-------------:|-----------:|-----------:|:--------------|:----------|
| `citizen_id` | `string` | `BG000020C` | `ZZ999936C` |  `9` | `9` | `0` | `0` | `0` |  `0` | `True` | `An external identifier for the bank customer, typically the national insurance number`|
