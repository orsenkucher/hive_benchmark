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

  @override
  Future<void> setUp() async {
    var dir = await getApplicationDocumentsDirectory();
    var homePath = path.join(dir.path, dbPath);

    db = await dbFactory.openDatabase(homePath);
  }

  @override
  Future<void> tearDown() async {
    db.close();
  }

  Future<int> _batchRead(String table, List<String> keys) async {
    var s = Stopwatch()..start();
    var store = StoreRef<String, dynamic>(table);

    await db.transaction((tx) async {
      for (var key in keys) {
        await store.record(key).get(tx);
      }
    });

    s.stop();
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
    var store = StoreRef<String, int>(TABLE_NAME_INT);

    await db.transaction((tx) async {
      for (var k in entries.keys) {
        final v = entries[k];
        await store.record(k).put(tx, v);
      }
    });

    s.stop();
    return s.elapsedMilliseconds;
  }

  @override
  Future<int> batchWriteString(Map<String, String> entries) async {
    var s = Stopwatch()..start();
    var store = StoreRef<String, String>(TABLE_NAME_STR);

    await db.transaction((tx) async {
      for (var k in entries.keys) {
        final v = entries[k];
        await store.record(k).put(tx, v);
      }
    });

    s.stop();
    return s.elapsedMilliseconds;
  }

  Future<int> _deleteFromTable(String table, List<String> keys) async {
    var s = Stopwatch()..start();
    var store = StoreRef<String, dynamic>(table);

    await db.transaction((tx) async {
      for (var key in keys) {
        await store.record(key).delete(tx);
      }
    });

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
