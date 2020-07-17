package main.java;

public class DBConnUrl {
    public static void main(String[] args){
        String inputUrl = "jdbc:mysql://db_usr_mgmt:3306/USR_MGMT?allowMultiQueries=true";//jdbc:mysql://localhost/dvja";//"jdbc:postgresql://localhost:5432/pg_rest";//"jdbc:db2:myDBName";
        //"jdbc:db2://localhost:6789/myDBName";//"jdbc:postgresql://localhost:5432/pg_rest";//"jdbc:oracle:thin:@10.11.96.62:1522:orcl";
        //"jdbc:mysql://localhost/restful_api?useSSL=false";
        //"jdbc:oracle:thin:@10.11.96.62:1522:orcl";
        //"jdbc:oracle:thin:@localhost:1521:xe";
        //"connectionUrl=jdbc:mysql://localhost:3306/notes_app?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8&allowMultiQueries=true&useSSL=false";
        //"jdbc:postgresql://localhost:5432/pg_rest";
        //"jdbc:oracle:thin:@10.11.96.62:1522:orcl";
        //"jdbc:mysql://localhost/restful_api?useSSL=false"; //working
        //"http://10.5.0.5:5100/http/generic/user/address"
        String dbType = inputUrl.substring(inputUrl.indexOf(':') + 1, inputUrl.indexOf(':', inputUrl.indexOf(':') + 1));
        String url = null;
        switch (dbType){
            case "mysql" :
                url = getHttpServerNameAndPort1(inputUrl);
                System.out.println("mysql case");
            break;
            case "postgresql" :
                url = getHttpServerNameAndPort1(inputUrl);
                System.out.println("postgresql case");
                break;
            case "oracle" :
                url = getHttpServerNameAndPort2(inputUrl);
                System.out.println("oracle case");
                break;
            case "db2" :
                System.out.println("db2 case");
                url = getHttpServerNameAndPort1(inputUrl);
                break;

        }
        //String url = getHttpServerNameAndPort1(inputUrl);
        System.out.println("url : " + url);
        int endLength = (inputUrl.indexOf('?') != -1) ? inputUrl.indexOf('?') : inputUrl.length();
        String databaseName = inputUrl.substring(inputUrl.indexOf(url) + url.length() + 1, endLength);
        System.out.println("Database name : " + databaseName);
    }

    private static String getHttpServerNameAndPort3(String url) {
        try {
            url = url + "/";
            int i = url.indexOf("://");
            url = url.substring(i + 3);
            i = url.indexOf('/');
            url = url.substring(0, i);
            return url;
        } catch (Exception ex) {
            System.out.println("Exception while parsing the http serviceUrl " + url + " from the agent, Hence ignoring");
            return "NA";
        }
    }

    private static String getHttpServerNameAndPort1(String url) {
        try {
            //sample url : "jdbc:db2://localhost:6789/myDBName" , "jdbc:postgresql://localhost:5432/pg_rest" , "jdbc:mysql://localhost/restful_api?useSSL=false"
            int i = url.indexOf("://");
            if(i != -1){
                url = url + "/";
                url = url.substring(i + 3);
                i = url.indexOf('/');
                url = url.substring(0, i);
            }else {
                //sample url: "jdbc:db2:myDBName"
                url = url.substring(0, url.indexOf(':', url.indexOf(':') + 1));
            }
            return url;
        } catch (Exception ex) {
            System.out.println("Exception while parsing the http serviceUrl " + url + " from the agent, Hence ignoring");
            return "NA";
        }
    }
    private static String getHttpServerNameAndPort2(String url) {
        try {
            int i = url.indexOf(":@");
            url = url.substring(i + 2);
            url = url.substring(0, url.indexOf(':', url.indexOf(':') + 1) );
            return url;
        } catch (Exception ex) {
            System.out.println("Exception while parsing the http serviceUrl " + url + " from the agent, Hence ignoring");
            return "NA";
        }
    }
}
