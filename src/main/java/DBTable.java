package main.java;

import com.google.gson.annotations.Expose;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@NoArgsConstructor
public class DBTable implements Serializable {
    @Expose
    private String tableName;

    @Expose
    private List<String> columns;

    @Expose
    private long createCount;

    @Expose
    private long readCount;

    @Expose
    private long updateCount;

    @Expose
    private long deleteCount;
}

