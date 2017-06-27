package com.alibaba.datax.core;

import java.io.*;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.*;
/*
 * 全库生成odps工具
 * TODO 合并到datax，实现全库自动发现并同步
 */
public class odpssql_autobuilder {

    private static String create_sql_template="CREATE TABLE IF NOT EXISTS ${tableName}(${columnsSection}) partitioned by (pt string);"; //partitioned by (pt string) LIFECYCLE 30;

    private static boolean will_merge_sharded_name =false;

    private static String mysql2odps_template ="";

    private static Map<String, String> mysql2odpsTypeMap = new HashMap();

    private static Properties config = null;
    static {
        mysql2odpsTypeMap.put("BIGINT", "bigint");
        mysql2odpsTypeMap.put("BIGINT UNSIGNED", "bigint");
        mysql2odpsTypeMap.put("BIT", "bigint");//boolean?
        mysql2odpsTypeMap.put("TINYINT", "bigint");
        mysql2odpsTypeMap.put("TINYINT UNSIGNED", "bigint");
        mysql2odpsTypeMap.put("SMALLINT", "bigint");
        mysql2odpsTypeMap.put("SMALLINT UNSIGNED", "bigint");
        mysql2odpsTypeMap.put("MEDIUMINT", "bigint");
        mysql2odpsTypeMap.put("MEDIUMINT UNSIGNED", "bigint");
        mysql2odpsTypeMap.put("INT", "bigint");
        mysql2odpsTypeMap.put("INT UNSIGNED", "bigint");

        mysql2odpsTypeMap.put("FLOAT", "double");
        mysql2odpsTypeMap.put("DOUBLE", "double");
        mysql2odpsTypeMap.put("DECIMAL", "double");

        mysql2odpsTypeMap.put("DATETIME", "datetime");
        mysql2odpsTypeMap.put("DATE", "datetime");
        mysql2odpsTypeMap.put("TIME", "datetime");
        mysql2odpsTypeMap.put("TIMESTAMP", "datetime");

        mysql2odpsTypeMap.put("VARCHAR", "string");
        mysql2odpsTypeMap.put("CHAR", "string");
        mysql2odpsTypeMap.put("TINYBLOB", "string");
        mysql2odpsTypeMap.put("TINYTEXT", "string");
        mysql2odpsTypeMap.put("BLOB", "string");
        mysql2odpsTypeMap.put("TEXT", "string");
        mysql2odpsTypeMap.put("MEDIUMBLOB", "string");
        mysql2odpsTypeMap.put("MEDIUMTEXT", "string");
        mysql2odpsTypeMap.put("LOGNGBLOB", "string");
        mysql2odpsTypeMap.put("LONGTEXT", "string");

        mysql2odpsTypeMap.put("BOOLEAN", "boolean");
        mysql2odpsTypeMap.put("BOOL", "boolean");
    }

    /**
     * @param args
     */
    public static void main(String[] args)   {
        int exitCode = 0;
        try {
            loadConfig("config.ini");
//            System.out.println(config);
            if(!isBlank(config.getProperty("CREATE_SQL_TEMPLATE")))
                create_sql_template=config.getProperty("CREATE_SQL_TEMPLATE");

            String prop_merge_sharded_name=config.getProperty("MERGE_SHARDED_NAME");
            if(!isBlank(prop_merge_sharded_name)&& "true".equalsIgnoreCase(prop_merge_sharded_name))
                will_merge_sharded_name =true;

            mysql2odps_template=loadTemplate(config.getProperty("MYSQL2ODPS_TEMPLATE"));
//            System.out.println(mysql2odps_template);

            readDbAndGenerateSql(config.getProperty("DATABASE_SERVER"),config.getProperty("DATABASE_NAME"),config.getProperty("DB_USERNAME"),config.getProperty("DB_PASSWORD"));
        } catch (Throwable e) {
            exitCode = 1;
//            LOG.error("\n\n错误原因是:\n" + e);
            e.printStackTrace();
            System.exit(exitCode);
        }
        System.exit(exitCode);
    }

    public static void readDbAndGenerateSql(String dbServer, String dbName, String userName, String password ) throws Throwable {
        String odps_db_name=dbName;
//        if(will_merge_sharded_name){
//            odps_db_name=dbName.replaceAll("(.*)_\\d+", "$1");
//        }

        File sqlFile=new File(odps_db_name+".sql");
        if(!sqlFile.exists()){
            sqlFile.createNewFile();
        }
        FileWriter fw =  new FileWriter(sqlFile);

        String  jdbcUrl="jdbc:mysql://"+dbServer+"/"+dbName+"?useUnicode=true&amp;characterEncoding=utf-8";//"za_fcp_bi","za_fcp_bi_169707"

        Connection conn = DriverManager.getConnection(jdbcUrl, userName,password);

        DatabaseMetaData dbmeta = conn.getMetaData();//获取数据库的元数据
        ResultSet rs = dbmeta.getTables(null, null, null, null);

        String prev_createTableOdpsSql="";
        String tableList="";
        int idx = 1;
        while (rs.next()) {
            if (idx == 1) {
                System.out.println("|库名：" + rs.getString(1));
                System.out.println("+----------------+");
            }
            String tableName = rs.getString("TABLE_NAME");
            System.out.println("|表" + (idx++) + ":" + tableName);
            ResultSet trs = dbmeta.getColumns(null, null, tableName, null);//获取表的元数据
            Map<String, String> columns = new LinkedHashMap<String, String>();
            while ((trs.next())) {
                String columnName = trs.getString("COLUMN_NAME");
                String columnClass = trs.getString("TYPE_NAME");
                columns.put(columnName, columnClass);
            }
            StringBuilder sb = new StringBuilder();
            StringBuilder sb_columns = new StringBuilder();
            for (Map.Entry<String, String> entry : columns.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                sb.append(key).append(' ').append(mysql2odpsTypeMap.get(value));
                sb.append(',').append(' ');
                sb_columns.append('"').append(key).append('"').append(',');
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.deleteCharAt(sb.length() - 1);
            sb_columns.deleteCharAt(sb_columns.length() - 1);
            trs.close();

            fw.write("--convert from mysql table "+tableName+"\r\n");

            String odps_table_name=tableName;
            if(will_merge_sharded_name){
                odps_table_name=tableName.replaceAll("(.*)_\\d+", "$1");
            }
            String createTableOdpsSql=create_sql_template.replace("${tableName}",odps_table_name).replace("${columnsSection}",sb.toString())+"\r\n";
            System.out.println("createTableOdpsSql=" + createTableOdpsSql);
//            System.out.println("prev_createTableOdpsSql=" + prev_createTableOdpsSql);
//            System.out.println(prev_createTableOdpsSql.equals(createTableOdpsSql));
            if(prev_createTableOdpsSql.equals(createTableOdpsSql)) {
                tableList+=(","+dbName+"."+tableName);
            }else{
                fw.write(createTableOdpsSql);
                tableList=dbName+"."+tableName;
            }
            prev_createTableOdpsSql=createTableOdpsSql;

            if(isBlank(mysql2odps_template))
                continue;

            //生成json文件
            File odpsFile=new File("import."+odps_table_name+".mysql2odps.json.template");
            if(!odpsFile.exists()){
                odpsFile.createNewFile();
            }
            FileWriter mysql2odpsWriter =  new FileWriter(odpsFile);
//            BufferedWriter mysql2odpsWriter = new BufferedWriter(mysql2odpsWriter);

            String mysql2odps=mysql2odps_template.replace("{DATABASE_SERVER}", config.getProperty("DATABASE_SERVER"))
                    .replace("{DATABASE_NAME}", dbName)
                    .replace("{DB_PASSWORD}", config.getProperty("DB_PASSWORD"))
                    .replace("{DB_USERNAME}", config.getProperty("DB_USERNAME"))
                    .replace("{TABLE_LIST}", tableList)
                    .replace("{COLUMN_LIST}", sb_columns.toString())
                    .replace("{ODPS_TABLE_NAME}", odps_table_name);

            mysql2odpsWriter.write(mysql2odps);
            mysql2odpsWriter.flush();
            mysql2odpsWriter.close();
        }

        fw.flush();
        fw.close();
        rs.close();
        conn.close();
    }

    public static boolean isBlank(String str) {
        int strLen;
        if(str != null && (strLen = str.length()) != 0) {
            for(int i = 0; i < strLen; ++i) {
                if(!Character.isWhitespace(str.charAt(i))) {
                    return false;
                }
            }

            return true;
        } else {
            return true;
        }
    }

    public static void loadConfig(String configFile) {
        File file=new File(configFile);
        FileInputStream fis=null;
        config=new Properties();
        if(file.exists()){
            try {
                fis=new FileInputStream(file);
                config.load(fis);
            } catch (Exception e) {
                System.out.println("error load checkpointFile, skipped.");
            }finally {
                try {
                    if(fis != null) {
                        fis.close();
                    }
                } catch (IOException var2) {
                    ;
                }
            }
        }
    }


    public static String loadTemplate(String templateFile) {
        File file=new File(templateFile);
        FileInputStream fis=null;
        if(file.exists()){
            try {
                fis=new FileInputStream(file);
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                // 一次读多个字节
                StringBuilder sb = new StringBuilder();
                byte[] tempbytes = new byte[4096];
                int byteread = 0;
                // 读入多个字节到字节数组中，byteread为一次读入的字节数
                while ((byteread = fis.read(tempbytes)) != -1) {
                    bos.write(tempbytes, 0, byteread);
                }

                bos.close();
                sb.append(bos.toString());
                return  sb.toString();
            } catch (Exception e) {
                System.out.println("error load templateFile, skipped.");
            }finally {
                try {
                    if(fis != null) {
                        fis.close();
                    }
                } catch (IOException e) {
                }
            }
        }

        return "";
    }
}