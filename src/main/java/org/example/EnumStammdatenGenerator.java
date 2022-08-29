package org.example;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.ContextEnabled;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.jooq.ForeignKey;
import org.jooq.UniqueKey;
import org.jooq.codegen.GeneratorStrategy;
import org.jooq.codegen.JavaGenerator;
import org.jooq.codegen.JavaWriter;
import org.jooq.meta.ConstraintDefinition;
import org.jooq.meta.Database;
import org.jooq.meta.ForeignKeyDefinition;
import org.jooq.meta.SchemaDefinition;
import org.jooq.meta.TableDefinition;
import org.jooq.meta.UniqueKeyDefinition;
import org.jooq.tools.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class EnumStammdatenGenerator extends JavaGenerator {

    private static final Logger log = LoggerFactory.getLogger(EnumStammdatenGenerator.class);

    private Set<String> getTableNameTriggers(Properties properties) {
        return new HashSet<>(properties.keySet()).stream()
                .map(properties::get)
                .map(Object::toString)
                .collect(Collectors.toSet());
    }

    /**
     * Generates enum from master tables.<br>
     * Each master data table must have exactly one primary key which spans across exactly one column (the datatype does not matter).<br>
     * Each master data table can have infinitely many additional columns but as of now only a handful datatypes are supported:
     * <ul>
     *     <li>any DB type that converts to a Java String (that includes Clob)</li>
     *     <li>any DB type that converts to a Java Integer</li>
     *     <li>TinyInt/Byte DB type to Java Boolean</li>
     * </ul>
     * @param schema to generate
     */
    @Override
    protected void generateSchema(SchemaDefinition schema) {
        log.info("Generating enums");
        log.info("--------------------------------------");
        Database db = schema.getDatabase();
        Properties properties = db.getProperties();
        log.info(properties.toString());
        Set<String> tableNameTriggers = getTableNameTriggers(db.getProperties());
        db.getTables(schema).stream()
                .filter(tableDefinition -> tableNameTriggers.stream().anyMatch(tableNameTrigger -> tableDefinition.getName().equalsIgnoreCase(tableNameTrigger)))
                .forEach(tableDefinition -> {
                    //preliminary checks
                    if(tableDefinition.getPrimaryKey().getKeyColumns().size() != 1) {
                        log.warn("The primary key '{}' of the table '{}' spans over multiple columns, but only one column is allowed for master data.",
                                tableDefinition.getPrimaryKey().getName(),
                                tableDefinition.getName());
                        log.warn("Skipping enum generation for the table '{}'", tableDefinition.getName());
                        return;
                    }

                    String enumName = StringUtils.toCamelCase(tableDefinition.getName()) + "Enum";
                    File file = new File(getFile(schema).getParentFile().getAbsolutePath(), enumName + ".java");
                    file.getParentFile().mkdirs();
                    try {
                        file.createNewFile();
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to create package files for enum generation.", e);
                    }
                    JavaWriter out = new JavaWriter(file, enumName + ".java");
                    log.info("Generating enum: {}.java [input={}, output={}]", enumName, tableDefinition.getName(), enumName);
                    printPackage(out, schema);

                    out.println(String.format("public enum %s {", enumName));

                    try {
                        ResultSet resultSet = db.getConnection().prepareStatement(String.format("SELECT * FROM %s.%s", schema.getName(), tableDefinition.getName())).executeQuery();
                        Map<String, String> variableTypes = new LinkedHashMap<>();
                        while(resultSet.next()) {
                            int colCount = resultSet.getMetaData().getColumnCount();
                            for(int col=1; col<=colCount; col++) {
                                String column = resultSet.getString(col);
                                // this is the primary key for this master data table
                                if(tableDefinition.getColumn(resultSet.getMetaData().getColumnLabel(col)).getPrimaryKey() != null) {
                                    String name = resultSet.getString(col).toUpperCase();
                                    name = toValidJavaEnumName(name);
                                    out.print(String.format("%s(", name));
                                    continue;
                                }
                                //this is any additional column belonging to the enums constructor parameter
                                String delimiter = col == colCount ? "" : ", ";
                                String columnClassName = Class.forName(resultSet.getMetaData().getColumnClassName(col)).getSimpleName();
                                log.info(columnClassName);
                                String columnLabel = resultSet.getMetaData().getColumnLabel(col);
                                if(columnClassName.equals("Clob") || columnClassName.equals("String")) {
                                    variableTypes.put(StringUtils.toLC(columnLabel), "String");
                                    out.print(String.format("\"%s\"", column));
                                }
                                else if(columnClassName.equals("Byte")) {
                                    variableTypes.put(StringUtils.toLC(columnLabel), "boolean");
                                    boolean toWrite = Byte.parseByte(column) != 0;
                                    out.print(Boolean.toString(toWrite));
                                }
                                else {
                                    variableTypes.put(StringUtils.toLC(columnLabel), columnClassName);
                                    out.print(column);
                                }
                                out.print(delimiter);
                            }
                            String s = resultSet.isLast() ? ";" : ",";
                            out.println(String.format(")%s", s));
                        }

                        out.println();

                        //class variables
                        variableTypes.forEach((varName, varType) -> {
                            out.println(String.format("private final %s %s;", varType, varName));
                        });

                        out.println();

                        //enum constructor definition
                        String constructorParameters = variableTypes.entrySet().stream()
                                .map(varNamevarTypeEntry -> String.format("%s %s", varNamevarTypeEntry.getValue(), varNamevarTypeEntry.getKey()))
                                .collect(Collectors.joining(","));
                        out.println(String.format("private %s(%s) {", enumName, constructorParameters));
                        variableTypes.forEach((varName, varType) -> {
                            out.println(String.format("this.%s = %s;", varName, varName));
                        });
                        out.println("}");

                        out.println();

                        //getter methods for class variables
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
                    } catch (SQLException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }
        });

        log.info("--------------------------------------");

        super.generateSchema(schema);
    }

    private String toValidJavaEnumName(String name) {
        if(!Character.isJavaIdentifierStart(name.charAt(0))) {
            name = "_" + name;
        }
        char[] nameArray = name.toCharArray();
        for(int i=0; i<nameArray.length; i++) {
            if(!Character.isJavaIdentifierPart(nameArray[i])) {
                nameArray[i] = '_';
            }
        }
        return String.copyValueOf(nameArray);
    }
}
