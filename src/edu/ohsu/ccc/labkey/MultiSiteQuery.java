package edu.ohsu.ccc.labkey;

import au.com.bytecode.opencsv.CSVWriter;
import ccc.ohsu.edu.dataSource.xml.ColumnMapping;
import ccc.ohsu.edu.dataSource.xml.DataSourceDocument;
import ccc.ohsu.edu.dataSource.xml.LabkeyInstance;
import ccc.ohsu.edu.dataSource.xml.LabkeyInstancesType;
import org.apache.commons.lang3.StringUtils;
import org.labkey.remoteapi.Connection;
import org.labkey.remoteapi.collections.CaseInsensitiveHashMap;
import org.labkey.remoteapi.query.Filter;
import org.labkey.remoteapi.query.SelectRowsCommand;
import org.labkey.remoteapi.query.SelectRowsResponse;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.util.*;

/**
 * Created by bimber on 4/1/2015.
 */
public class MultiSiteQuery
{
    private List<LabKeyDataSource> _dataSources;
    private List<String> _columns;

    public MultiSiteQuery(File xml) throws Exception
    {
        parseXml(xml);
    }

    private void parseXml(File xmlFile) throws Exception
    {
        _dataSources = new ArrayList<>();
        _columns = new ArrayList<>();

        DataSourceDocument doc = DataSourceDocument.Factory.parse(xmlFile);
        LabkeyInstancesType lk = doc.getDataSource().getLabkeyInstances();
        for (LabkeyInstance l : lk.getLabkeyInstanceArray())
        {
            _dataSources.add(new LabKeyDataSource(l));
        }

        _columns.addAll(Arrays.asList(doc.getDataSource().getColumns().getColumnArray()));
    }

    public void executeQuery(File output, String userName, String password, boolean hideHeaders, boolean includeSiteName, String[] filterStrings) throws Exception
    {
        try (CSVWriter writer = new CSVWriter(output == null ? new BufferedWriter(new OutputStreamWriter(System.out)) : new FileWriter(output), '\t', CSVWriter.NO_QUOTE_CHARACTER))
        {
            if (!hideHeaders) {
                List<String> toWrite = new ArrayList<>();
                if (includeSiteName)
                {
                    toWrite.add("SiteName");
                }
                toWrite.addAll(_columns);

                writer.writeNext(toWrite.toArray(new String[toWrite.size()]));
            }

            for (LabKeyDataSource ds : _dataSources)
            {
                SelectRowsCommand sr = new SelectRowsCommand(ds.getSchemaName(), ds.getQueryName());
                if (filterStrings != null && filterStrings.length > 0)
                {
                    for (Filter f : ds.parseFilters(filterStrings))
                    {
                        sr.addFilter(f);
                    }
                }

                List<String> translatedCols = new ArrayList<>();
                for (String colName : _columns)
                {
                    translatedCols.add(ds.resolveColumnName(colName));
                }
                sr.setColumns(translatedCols);
                sr.setExtendedFormat(false);

                SelectRowsResponse srr = sr.execute(new Connection(ds.getBaseUrl(), userName, password), ds.getContainerPath());

                for (Map<String, Object> r : srr.getRows())
                {
                    r = new CaseInsensitiveHashMap<>(r);
                    List<String> row = new ArrayList<>();
                    if (includeSiteName)
                    {
                        row.add(ds.getName());
                    }

                    for (String colName : _columns)
                    {
                        if (r.get(ds.resolveColumnName(colName)) != null)
                        {
                            row.add(r.get(ds.resolveColumnName(colName)).toString());
                        }
                        else
                        {
                            row.add("");
                        }
                    }

                    writer.writeNext(row.toArray(new String[row.size()]));
                }
            }
        }
    }

    private class LabKeyDataSource
    {
        private String _name;
        private String _baseUrl;
        private String _containerPath;
        private String _schemaName;
        private String _queryName;
        private Map<String, String> _aliasMap;

        public LabKeyDataSource(LabkeyInstance ds)
        {
            _name = ds.getName();
            _baseUrl = ds.getBaseUrl();
            _containerPath = ds.getContainerPath();
            _schemaName = ds.getSchemaName();
            _queryName = ds.getQueryName();

            _aliasMap = new CaseInsensitiveHashMap<>();
            if (ds.getColumnMappings() != null)
            {
                for (ColumnMapping m : ds.getColumnMappings().getColumnMappingArray())
                {
                    _aliasMap.put(m.getColumnName(), StringUtils.trimToNull(m.getFieldKey()));
                }
            }
        }

        public String getBaseUrl() {
            return _baseUrl;
        }

        public String getContainerPath() {
            return _containerPath;
        }

        public String getSchemaName() {
            return _schemaName;
        }

        public String getQueryName() {
            return _queryName;
        }

        public String getName() {
            return _name;
        }

        public void setName(String name) {
            _name = name;
        }

        public String resolveColumnName(String colName) {
            return _aliasMap.containsKey(colName) ? _aliasMap.get(colName) : colName;
        }

        private List<Filter> parseFilters(String[] filterStrings)
        {
            List<Filter> ret = new ArrayList<>();
            for (String filterStr : filterStrings)
            {
                if (filterStr == null || "".equals(filterStr.trim()))
                {
                    return null;
                }

                List<String> tokens = new ArrayList<>();
                tokens.addAll(Arrays.asList(filterStr.split("=")));

                String colName = tokens.remove(0);
                Filter.Operator op = Filter.Operator.EQUAL;
                if (colName.contains("~"))
                {
                    String[] split = colName.split("~");
                    colName = split[0];
                    for (Filter.Operator o : Filter.Operator.values())
                    {
                        if (o.getUrlKey().equals(split[1]))
                        {
                            op = o;
                            break;
                        }
                    }

                    if (op == null)
                    {
                        throw new RuntimeException("Unknown operator: [" + colName + "], [" + split[1] + "]");
                    }
                }

                ret.add(new Filter(resolveColumnName(colName), StringUtils.join(tokens, "="), op));
            }

            return ret;
        }
    }
}
