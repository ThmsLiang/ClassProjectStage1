import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.PathUtil;
import com.apple.foundationdb.tuple.Tuple;

/**
 * TableManagerImpl implements interfaces in {#TableManager}. You should put your implementation
 * in this class.
 */
public class TableManagerImpl implements TableManager{

  private DirectorySubspace rootDirectory = null;
  private Database db;
  private HashMap<String, TableMetadata> tableMetaDatas = new HashMap<>();
  public TableManagerImpl() {
    FDB fdb =FDB.selectAPIVersion(710);
    db = null;

    try{
      db = fdb.open();
    }catch (Exception e){
      System.out.println("ERROR: the database is not successfully opened: " + e);
    }

    try{
      rootDirectory = DirectoryLayer.getDefault().createOrOpen(db, PathUtil.from("Tables")).join();
    }catch (Exception e){
      System.out.println("ERROR: the root directory is not successfully opened: " + e);
    }

  }

  @Override
  public StatusCode createTable(String tableName, String[] attributeNames, AttributeType[] attributeType,
                         String[] primaryKeyAttributeNames) {
    // your code

    // Check table already exists
    if (tableMetaDatas.containsKey(tableName)) {
      System.out.println("TABLE " + tableName + " ALREADY EXIST!");
      return StatusCode.ATTRIBUTE_ALREADY_EXISTS;
    }

    // check primary keys null
    if (primaryKeyAttributeNames.length == 0) {
      System.out.println("PRIMARY KEY CANNOT BE NULL");
      return StatusCode.TABLE_CREATION_NO_PRIMARY_KEY;
    }

    // Check attribute type correct
    for(AttributeType type: attributeType) {
      if(type == null) {
        System.out.println("ATTRIBUTE TYPE CANNOT BE NULL");
        return StatusCode.ATTRIBUTE_TYPE_NOT_SUPPORTED;
      }
    }

    // check duplicate attribute name
    Set<String> set = new HashSet<>();
    for(String name: attributeNames){
      if(!set.add(name)) {
        System.out.println("CANNOT HAVE DUPLICATE ATTRIBUTES");
        return StatusCode.ATTRIBUTE_ALREADY_EXISTS;
      }
    }

    // Finally create table
    TableMetadata metadata = new TableMetadata(attributeNames, attributeType, primaryKeyAttributeNames);
    tableMetaDatas.put(tableName, metadata);
    rootDirectory.create(db, PathUtil.from(tableName)).join();


    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode deleteTable(String tableName) {
    // your code

    // check table exist
    if(!tableMetaDatas.containsKey(tableName)) {
      System.out.println("TABLE NOT EXIST");
      return StatusCode.TABLE_NOT_FOUND;
    }

    // check tablename empty
    if(tableName == null) {
      return StatusCode.SUCCESS;
    }

    // delete table
    rootDirectory.remove(db, PathUtil.from(tableName)).join();
    tableMetaDatas.remove(tableName);
    return StatusCode.SUCCESS;
  }

  @Override
  public HashMap<String, TableMetadata> listTables() {
    // your code
    return tableMetaDatas;
  }

  @Override
  public StatusCode addAttribute(String tableName, String attributeName, AttributeType attributeType) {
    // your code

    // check attribute already exists
    if(tableMetaDatas.get(tableName).doesAttributeExist(attributeName)) {
      System.out.println("ATTRIBUTE NAME ALREADY EXISTS");
      return StatusCode.ATTRIBUTE_ALREADY_EXISTS;
    }

    // check type
    if(attributeType == null) {
      System.out.println("UNSUPPORTED ATTRIBUTE TYPE");
      return StatusCode.ATTRIBUTE_TYPE_NOT_SUPPORTED;
    }

    tableMetaDatas.get(tableName).addAttribute(attributeName, attributeType);
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAttribute(String tableName, String attributeName) {
    // your code

    // check if attribute exist
    if(!tableMetaDatas.get(tableName).doesAttributeExist(attributeName)){
      System.out.println("ATTRIBUTE NOT EXIST");
      return StatusCode.ATTRIBUTE_NOT_FOUND;
    }

    tableMetaDatas.get(tableName).setAttributes(null);
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAllTables() {
    // your code
    for(String subdir: rootDirectory.list(db).join()) {
      rootDirectory.remove(db, PathUtil.from(subdir)).join();
    }
    tableMetaDatas.clear();
    return StatusCode.SUCCESS;
  }
}
