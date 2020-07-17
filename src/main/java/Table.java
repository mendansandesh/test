package main.java;

import com.google.gson.annotations.Expose;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BinaryOperator;

        /*
         *  Created by Vishnu G Singhal on 2018-09-27.
         */
        import com.google.gson.annotations.Expose;
        import lombok.Data;
        import lombok.NoArgsConstructor;
        import org.apache.spark.api.java.function.Function2;
        import org.slf4j.Logger;
        import org.slf4j.LoggerFactory;

        import java.io.Serializable;
        import java.util.HashMap;
        import java.util.Map;
        import java.util.function.BinaryOperator;

@Data
@NoArgsConstructor
public class Table implements Serializable {
    @Expose
    private String tableName;

    @Expose
    private long createCount;

    @Expose
    private long readCount;

    @Expose
    private long updateCount;

    @Expose
    private long deleteCount;

    @Expose
    private Map<String, String> columnMap;

    private static final Logger log = LoggerFactory.getLogger(Table.class);
    public static final Function2<Table,Table,Table> tableReducerS = (Function2<Table,Table,Table>) (t1, t2) ->
    {
        Map<String,String> combinedColMap = new HashMap();
        for(Map.Entry<String,String> entry :t1.getColumnMap().entrySet()){
            combinedColMap.put(entry.getKey(),"");
        }
        for(Map.Entry<String,String> entry :t2.getColumnMap().entrySet()){
            combinedColMap.put(entry.getKey()," ");
        }
        t1.setReadCount(t1.getReadCount() + t2.getReadCount());
        t1.setUpdateCount(t1.getUpdateCount() + t2.getUpdateCount());
        t1.setDeleteCount(t1.getDeleteCount() + t2.getDeleteCount());
        t1.setCreateCount(t1.getCreateCount() + t2.getCreateCount());
        t1.setColumnMap(combinedColMap);
        return t1;
    };

    public static final BinaryOperator<Table> tableReducer = (p1, p2) ->
    {
        try {
            return tableReducerS.call(p1, p2);
        }catch (Exception e){
            log.error("Exception occured while reducing two tables {} ",e.getMessage());
            return p1;
        }
    };


}
