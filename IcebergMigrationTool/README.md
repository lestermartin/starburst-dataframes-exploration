# Iceberg migration tool

The Jupyter [migration tool notebook](./Migrate2Iceberg.ipynb) works with 
[Starburst Galaxy](https://www.starburst.io/platform/starburst-galaxy/) clusters configured with at least one 
[data lake catalog](https://docs.starburst.io/starburst-galaxy/data-engineering/working-with-data-lakes/storage.html) that has 
[Apache Hive tables](https://docs.starburst.io/starburst-galaxy/data-engineering/working-with-data-lakes/table-formats/gl-hive.html) which need to be migrated to 
[Apache Iceberg](https://docs.starburst.io/starburst-galaxy/data-engineering/working-with-data-lakes/table-formats/gl-iceberg.html) ones.

If you don't have access to a 
[Jupyter instance](https://jupyter.org/install) available, surf on over to 
[starburstdata/pystarburst-examples](https://github.com/starburstdata/pystarburst-examples) 
and click on the `Launch|Binder` icon to quickly stand up an ephemeral instance that you can utilize.

Just upload the [notebook](./Migrate2Iceberg.ipynb) into your Jupyter instance and you're off to the races.

**Note:** 
*Currently, this migration tool only tackles the "easy path" of in-place migrations that work without exceptions 
and CTAS "shadow" migrations that don't try to setup up advanced features like partitioning, bucketing, and sorting.*

The initial ["happy path" SQL](./test_scenario_01_happy_path.sql) can be run in your 
Starburst Galaxy instance to have some tests that are known to work at time.

Of course, I'm always glad for any help with finding bugs and/or enhancing this 
Iceberg migration tool.

I put a [blog post wrapper](https://lestermartin.wordpress.com/2024/04/25/hive-to-iceberg-migration-tool-rev1/) 
around this tool and below is a recording of me demoing the tool in its current state.

[![migration tool demo (rev1)](http://img.youtube.com/vi/pKoyKP6DSbI/0.jpg)](http://www.youtube.com/watch?v=pKoyKP6DSbI)
