using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.Common;
using TSVCEO.DataModelling;

namespace TSVCEO.DataModelling.SQLite
{
    public class SQLiteDbInitializer : SQLDbInitializer
    {
        public override bool ColumnDataIsVariant { get { return true; } }
        public override SQLQuoteType IdentifierQuoteType { get { return SQLQuoteType.Quote; } }
        public override bool SupportsAddDropConstraint { get { return false; } }
        public override bool SupportsAlterColumn { get { return false; } }
        public override bool SupportsDropColumn { get { return false; } }
        public override bool SupportsCreateTableWithConstraints { get { return true; } }

        protected override string GetBaseTypeName(IColumnType type)
        {
            if (type.DataType == DbType.Single || 
                type.DataType == DbType.Double ||
                type.DataType == DbType.Decimal)
            {
                return "REAL";
            }
            else if (type.DataType == DbType.Boolean ||
                     type.DataType == DbType.SByte ||
                     type.DataType == DbType.Int16 ||
                     type.DataType == DbType.Int32 ||
                     type.DataType == DbType.Int64 ||
                     type.DataType == DbType.Byte ||
                     type.DataType == DbType.UInt16 ||
                     type.DataType == DbType.UInt32 ||
                     type.DataType == DbType.UInt64)
            {
                return "INTEGER";
            }
            else
            if (type.DataType == DbType.Binary)
            {
                return "BLOB";
            }
            else
            {
                return "TEXT";
            }
        }

        protected override string GetTypeName(IColumnType type)
        {
            return String.Format("{0}{1}",
                GetBaseTypeName(type),
                type.IsNullable ? "" : " NOT NULL"
            );
        }

        protected override IEnumerable<IColumnDef> GetBreakingChangedColumns(IEntityMap map, IEntityMap original)
        {
            return base.GetChangedColumns(map, original).Where(p => p.Old.Type.IsNullable && !p.New.Type.IsNullable).Select(p => p.New);
        }

        protected override IEnumerable<IColumnDef> GetNonBreakingChangedColumns(IEntityMap map, IEntityMap original)
        {
            return base.GetChangedColumns(map, original).Where(p => (p.New.Type.IsNullable || !p.Old.Type.IsNullable) && GetBaseTypeName(p.Old.Type) != GetBaseTypeName(p.New.Type)).Select(p => p.New);
        }

        protected override IEnumerable<IIndexMap> GetDroppedIndexes(IEntityMap map, IEntityMap original)
        {
            if (RequireCopyRenameTable(map, original) || map == null)
            {
                return original.Indexes;
            }
            else
            {
                return original == null ? new IIndexMap[0] : original.Indexes.Where(ix => !map.Indexes.Any(nix => nix.Columns.Equals(ix.Columns)));
            }
        }

        protected override IEnumerable<IIndexMap> GetAddedIndexes(IEntityMap map, IEntityMap original)
        {
            if (RequireCopyRenameTable(original, map) || original == null)
            {
                return map.Indexes;
            }
            else
            {
                return map == null ? new IIndexMap[0] : map.Indexes.Where(ix => !original.Indexes.Any(nix => nix.Columns.Equals(ix.Columns)));
            }
        }

        protected override IEnumerable<IUniqueKeyMap> GetAddedUniqueKeys(IEntityMap map, IEntityMap original)
        {
            return map == null ? new IUniqueKeyMap[0] : map.UniqueKeys.Where(ak => original == null || !original.UniqueKeys.Any(oak => oak.Equals(ak)));
        }

        protected override IEnumerable<IUniqueKeyMap> GetDroppedUniqueKeys(IEntityMap map, IEntityMap original)
        {
            return original == null ? new IUniqueKeyMap[0] : original.UniqueKeys.Where(ak => map == null || !map.UniqueKeys.Any(nak => nak.Equals(ak)));
        }

        protected override IEnumerable<IForeignKeyMap> GetAddedForeignKeys(IEntityMap map, IEntityMap original)
        {
            return map == null ? new IForeignKeyMap[0] : map.ForeignKeys.Where(fk => original == null || !original.ForeignKeys.Any(ofk => ofk.Equals(fk)));
        }

        protected override IEnumerable<IForeignKeyMap> GetDroppedForeignKeys(IEntityMap map, IEntityMap original)
        {
            return original == null ? new IForeignKeyMap[0] : original.ForeignKeys.Where(fk => map == null || !map.ForeignKeys.Any(nfk => nfk.Equals(fk)));
        }

        protected override IEnumerable<string> DDLBeforeModifyTables(IEnumerable<EntityMappingPair> mappairs)
        {
            if (mappairs.Any(p => p.Old == null || p.New == null || RequireCopyRenameTable(p.New, p.Old)))
            {
                return new string[]
                {
                    "PRAGMA foreign_keys=OFF"
                };
            }
            else
            {
                return new string[0];
            }
        }

        protected override IEnumerable<string> DDLAfterModifyTables(IEnumerable<EntityMappingPair> mappairs)
        {
            if (mappairs.Any(p => p.Old == null || p.New == null || RequireCopyRenameTable(p.New, p.Old)))
            {
                return new string[]
                {
                    "PRAGMA foreign_key_check",
                    "PRAGMA foreign_keys=ON"
                };
            }
            else
            {
                return new string[0];
            }
        }

        protected void AddColumnMap(IEntityMap map, string colname, string typename, bool notnull, bool pk)
        {
            Type type = typeof(string);

            if (typename == "INTEGER")
            {
                type = notnull ? typeof(long) : typeof(long?);
            }
            else if (typename == "REAL")
            {
                type = notnull ? typeof(double) : typeof(double?);
            }
            else if (typename == "BLOB")
            {
                type = typeof(byte[]);
            }

            IColumnMap colmap = new ColumnMap(map, colname, type);
            colmap.Column.Type.IsNullable = !notnull;

            map.Columns.Add(colmap);

            if (pk)
            {
                map.PrimaryKey = new PrimaryKeyMap(map, "PK_" + map.TableName, colmap.Column);
            }
        }
        
        protected void GetTableColumns(IEntityMap map, DbConnection conn)
        {
            using (DbCommand cmd = conn.CreateCommand())
            {
                cmd.CommandText = "PRAGMA table_info(" + EscapeTableName(map.TableName) + ")";

                using (DbDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string colname = reader["name"] as string;
                        string typename = reader["type"] as string;
                        bool notnull = (long)reader["notnull"] != 0;
                        bool pk = (long)reader["pk"] != 0;

                        AddColumnMap(map, colname, typename, notnull, pk);
                    }
                }
            }
        }

        protected IEnumerable<IColumnRef> GetIndexColumns(IEntityMap map, DbConnection conn, string name)
        {
            using (DbCommand cmd = conn.CreateCommand())
            {
                cmd.CommandText = "PRAGMA index_info(" + EscapeColumnName(name) + ")";

                using (DbDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string colname = reader["name"] as string;

                        yield return map.Columns.Single(c => c.Column.Name == colname).Column;
                    }
                }
            }
        }

        protected void AddUniqueKeyMap(IEntityMap map, DbConnection conn, string name)
        {
            IColumnRef[] cols = GetIndexColumns(map, conn, name).ToArray();
            IUniqueKeyMap uq = new UniqueKeyMap(map, name);

            foreach (IColumnRef colref in cols)
            {
                uq.Columns.Add(colref);
            }

            if (!map.UniqueKeys.Any(u => u.Columns.Equals(uq.Columns)))
            {
                map.UniqueKeys.Add(uq);
            }
        }
        
        protected void AddIndexMap(IEntityMap map, DbConnection conn, string name)
        {
            IColumnRef[] cols = GetIndexColumns(map, conn, name).ToArray();
            IIndexMap ix = new IndexMap(map, name);

            foreach (IColumnRef colref in cols)
            {
                ix.Columns.Add(colref);
            }

            map.Indexes.Add(ix);
        }

        protected void GetTableIndexes(IEntityMap map, DbConnection conn)
        {
            using (DbCommand cmd = conn.CreateCommand())
            {
                cmd.CommandText = "PRAGMA index_list(" + EscapeTableName(map.TableName) + ")";

                using (DbDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string name = reader["name"] as string;
                        bool unique = (long)reader["unique"] != 0;

                        if (unique)
                        {
                            AddUniqueKeyMap(map, conn, name);
                        }
                        else
                        {
                            AddIndexMap(map, conn, name);
                        }
                    }
                }
            }
        }

        protected void GetForeignKey(IEntityMap map, IEntityMap dstmap, long id, string from, string to)
        {
            string fkname = String.Format("FK_{0}_{1}", map.TableName, id);
            IForeignKeyMap fkmap = map.ForeignKeys.SingleOrDefault(fk => fk.KeyName == fkname);

            if (fkmap == null)
            {
                fkmap = new ForeignKeyMap(map, map.PrimaryKey.KeyColumn.Name, fkname);
                fkmap.ReferencedKey = new UniqueKeyMap(dstmap, fkname);
                map.ForeignKeys.Add(fkmap);
            }

            fkmap.Columns.Add(map.Columns.Single(c => c.Column.Name.ToLower() == from.ToLower()).Column);
            fkmap.ReferencedKey.Columns.Add(dstmap.Columns.Single(c => c.Column.Name.ToLower() == to.ToLower()).Column);
        }
        
        protected void GetForeignKeys(IEntityMap map, DbConnection conn, Dictionary<string, IEntityMap> maps)
        {
            using (DbCommand cmd = conn.CreateCommand())
            {
                cmd.CommandText = "PRAGMA foreign_key_list(" + EscapeTableName(map.TableName) + ")";

                using (DbDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        long id = (long)reader["id"];
                        string table = reader["table"] as string;
                        string from = reader["from"] as string;
                        string to = reader["to"] as string;

                        IEntityMap dstmap = maps[table];

                        GetForeignKey(map, dstmap, id, from, to);
                    }
                }
            }

            foreach (IForeignKeyMap fkmap in map.ForeignKeys)
            {
                fkmap.ReferencedKey = fkmap.ReferencedKey.Table.UniqueKeys.Single(uq => uq.Columns.Equals(fkmap.ReferencedKey.Columns));
            }
        }

        protected override IEnumerable<IEntityMap> GetEntityMaps(DbConnection conn)
        {
            List<string> tablenames = new List<string>();
            Dictionary<string, IEntityMap> tables = new Dictionary<string,IEntityMap>();

            using (DbCommand cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT tbl_name FROM sqlite_master WHERE type = 'table'";
                
                using (DbDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        tablenames.Add(reader["tbl_name"] as string);
                    }
                }
            }

            foreach (string tablename in tablenames)
            {
                IEntityMap map = new EntityMap(tablename);

                GetTableColumns(map, conn);
                GetTableIndexes(map, conn);

                tables[tablename] = map;
            }

            foreach (IEntityMap map in tables.Values)
            {
                GetForeignKeys(map, conn, tables);
            }

            return tables.Values;
        }
    }
}
