import 'dart:io';

import 'package:hive_benchmark/runners/runner.dart';
import 'package:path_provider/path_provider.dart';
import 'package:path/path.dart' as path;
import 'dart:async';

import 'package:sembast/sembast.dart';
import 'package:sembast/sembast_io.dart';

import '../benchmark.dart';

class SembastRunner implements BenchmarkRunner {
  @override
  String get name => 'Sembast';

  String dbPath = 'sample.db';
  DatabaseFactory dbFactory = databaseFactoryIo;
  Database db;
// TODO MERGE
  @override
  Future<void> setUp() async {
    var dir = await getApplicationDocumentsDirectory();
    var homePath = path.join(dir.path, dbPath);

    // final file = File(homePath);
    // if (await file.exists()) {
    //   await file.delete();
    // }

    db = await dbFactory.openDatabase(homePath);

    // db.execute(
    //     'CREATE TABLE $TABLE_NAME_STR (key TEXT PRIMARY KEY, value TEXT)');
    // db.execute(
    //     'CREATE TABLE $TABLE_NAME_INT (key TEXT PRIMARY KEY, value INTEGER)');
  }

  @override
  Future<void> tearDown() async {
    db.close();
  }

  Future<int> _batchRead(String table, List<String> keys) async {
    var s = Stopwatch()..start();
    // final stmt = db.prepare('SELECT * FROM $table WHERE key = ?');
    // var store = StoreRef.main();
    // var store = stringMapStoreFactory.store(table);
    var store = StoreRef<String, dynamic>(table);

    // for (var key in keys) {
    //   // final result = stmt.select([key]);
    //   // read all rows because that would be required during a real read
    //   // result.forEach((row) => row['value']);
    //   await store.record(key).get(db);
    // }

    await db.transaction((tx) async {
      for (var key in keys) {
        await store.record(key).get(tx);
      }
    });

    s.stop();
    // stmt.close();
    // store.
    return s.elapsedMilliseconds;
  }

  @override
  Future<int> batchReadInt(List<String> keys) async {
    return _batchRead(TABLE_NAME_INT, keys);
  }

  @override
  Future<int> batchReadString(List<String> keys) {
    return _batchRead(TABLE_NAME_STR, keys);
  }

  @override
  Future<int> batchWriteInt(Map<String, int> entries) async {
    var s = Stopwatch()..start();
    // final stmt = db.prepare(
    //     'INSERT OR REPLACE INTO $TABLE_NAME_INT (key, value) VALUES (?, ?)');
    // var store = StoreRef.main();
    // var store = stringMapStoreFactory.store(TABLE_NAME_INT);
    var store = StoreRef<String, int>(TABLE_NAME_INT);
    await db.transaction((tx) async {
      // entries.forEach((key, value) async {
      //   await store.record(key).put(db, value);
      //   // stmt.execute([key, value]);
      // });
      for (var k in entries.keys) {
        final v = entries[k];
        await store.record(k).put(tx, v);
      }
    });

    // stmt.close();
    s.stop();
    return s.elapsedMilliseconds;
  }

  @override
  Future<int> batchWriteString(Map<String, String> entries) async {
    var s = Stopwatch()..start();
    // final stmt = db.prepare(
    //     'INSERT OR REPLACE INTO $TABLE_NAME_STR (key, value) VALUES (?, ?)');
    // var store = StoreRef.main();
    var store = StoreRef<String, String>(TABLE_NAME_STR);
    // entries.forEach((key, value) {
    //   stmt.execute([key, value]);
    // });

    await db.transaction((tx) async {
      for (var k in entries.keys) {
        final v = entries[k];
        await store.record(k).put(tx, v);
      }
    });

    // stmt.close();
    s.stop();
    return s.elapsedMilliseconds;
  }

  Future<int> _deleteFromTable(String table, List<String> keys) async {
    var s = Stopwatch()..start();
    // final stmt = db.prepare('DELETE FROM $table WHERE key = ?');
    var store = StoreRef<String, dynamic>(table);

    // for (var key in keys) {
    //   // stmt.execute([key]);
    //   await store.record(key).delete(db);
    // }

    await db.transaction((tx) async {
      for (var key in keys) {
        await store.record(key).delete(tx);
      }
    });

    // stmt.close();
    s.stop();
    return s.elapsedMilliseconds;
  }

  @override
  Future<int> batchDeleteInt(List<String> keys) {
    return _deleteFromTable(TABLE_NAME_INT, keys);
  }

  @override
  Future<int> batchDeleteString(List<String> keys) {
    return _deleteFromTable(TABLE_NAME_STR, keys);
  }
}
