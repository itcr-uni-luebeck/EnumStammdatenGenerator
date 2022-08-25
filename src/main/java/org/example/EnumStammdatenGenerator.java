package org.example;

import org.jooq.codegen.JavaGenerator;
import org.jooq.codegen.JavaWriter;
import org.jooq.meta.Database;
import org.jooq.meta.SchemaDefinition;
import org.jooq.tools.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class EnumStammdatenGenerator extends JavaGenerator {

    private static final Logger log = LoggerFactory.getLogger(EnumStammdatenGenerator.class);

    @Override
    protected void generateSchema(SchemaDefinition schema) {
        log.info("Generating enums");
        log.info("--------------------------------------");

        Database db = schema.getDatabase();
        db.getTables(schema).stream()
                .filter(tableDefinition -> tableDefinition.getName().toLowerCase().contains("stammdaten"))
                .forEach(tableDefinition -> {
                    String enumName = StringUtils.toCamelCase(tableDefinition.getName()) + "Enum";
                    File file = new File(getFile(schema).getParentFile().getAbsolutePath(), enumName + ".java");
                    file.getParentFile().mkdirs();
                    try {
                        file.createNewFile();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    JavaWriter out = new JavaWriter(file, enumName + ".java");
                    log.info("Generating enum: {}.java [input={}, output={}]", enumName, tableDefinition.getName(), enumName);
                    printPackage(out, schema);

                    out.println(String.format("public enum %s {", enumName));

                    try {
                        // log.info(String.format("SELECT * FROM %s.%s", schema.getName(), tableDefinition.getName()));
                        ResultSet resultSet = db.getConnection().prepareStatement(String.format("SELECT * FROM %s.%s", schema.getName(), tableDefinition.getName())).executeQuery();
                        Map<String, String> variableTypes = new LinkedHashMap<>();
                        while(resultSet.next()) {
                            int colCount = resultSet.getMetaData().getColumnCount();
                            for(int col=1; col<=colCount; col++) {
                                String column = resultSet.getString(col);
                                // this is the primary key for this master data table
                                if(tableDefinition.getColumn(resultSet.getMetaData().getColumnLabel(col)).getPrimaryKey() != null) {
                                    String name = resultSet.getString(col).toUpperCase();
                                    out.print(String.format("%s(", name));
                                    continue;
                                }

                                String delimiter = col == colCount ? "" : ", ";
                                String columnClassName = Class.forName(resultSet.getMetaData().getColumnClassName(col)).getSimpleName();
                                if(columnClassName.equals("Clob")) {
                                    variableTypes.put(StringUtils.toLC(resultSet.getMetaData().getColumnLabel(col)), "String");
                                    out.print(String.format("\"%s\"", column));
                                }
                                else if(columnClassName.equals("Byte")) {
                                    variableTypes.put(StringUtils.toLC(resultSet.getMetaData().getColumnLabel(col)), "boolean");
                                    boolean toWrite = Byte.parseByte(column) != 0;
                                    out.print(Boolean.toString(toWrite));
                                }
                                else {
                                    variableTypes.put(StringUtils.toLC(resultSet.getMetaData().getColumnLabel(col)), columnClassName);
                                    out.print(column);
                                }
                                out.print(delimiter);

                            }
                            String s = resultSet.isLast() ? ";" : ",";
                            out.println(String.format(")%s", s));
                        }

                        out.println();

                        variableTypes.forEach((varName, varType) -> {
                            out.println(String.format("private final %s %s;", varType, varName));
                        });

                        out.println();

                        String constructorParameters = variableTypes.entrySet().stream()
                                .map(varNamevarTypeEntry -> String.format("%s %s", varNamevarTypeEntry.getValue(), varNamevarTypeEntry.getKey()))
                                .collect(Collectors.joining(","));
                        out.println(String.format("private %s(%s) {", enumName, constructorParameters));
                        variableTypes.forEach((varName, varType) -> {
                            out.println(String.format("this.%s = %s;", varName, varName));
                        });
                        out.println("}");

                        out.println();

                        variableTypes.forEach((varName, varType) -> {
                            if(varType.equals("boolean")) {
                                out.println(String.format("public %s is%s() {", varType, StringUtils.toUC(varName)));
                            }
                            else {
                                out.println(String.format("public %s get%s() {", varType, StringUtils.toUC(varName)));
                            }
                            out.println(String.format("return this.%s;", varName));
                            out.println("}");
                            out.println();
                        });

                        out.println("}");

                        closeJavaWriter(out);

                        log.info("--------------------------------------");
                        super.generateSchema(schema);
                    } catch (SQLException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }
        });
    }
}
