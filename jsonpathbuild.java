package spark;

import com.google.gson.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.hash.Hash;
import org.json.JSONArray;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class jsonpathbuild {

    public static void main(String[] args) {
        // JSON provided as input
        //String rawJson = "[{\"key\":\"val\",\"type\":\"myType\",\"someSubJson\":{\"key\":\"val\"}},{\"key\":\"val\",\"type\":\"myType\"}]";

        String rawJson = "{ \"claimdetails\": { \"tpl\" : [{}] } } ";

        // xpath for adding into first element of JSON array
        //String xpath = "$.[0].myName";

         List<String> paths  = new ArrayList();
         paths.add("claimdetails.patients.address.cityname");
         paths.add("claimdetails.patients.address.countrycode");

       // int len =  findLevel(xpath);

        Map<String,JSONObject> lastjs  = new HashMap<String, JSONObject>();
        Map<String,JSONObject> lastjs2  = new HashMap<String, JSONObject>();
        Map<String,JSONObject> lastjs3  = new HashMap<String, JSONObject>();
        Map<String,JSONObject> lastjs4  = new HashMap<String, JSONObject>();

        jsonpathbuild test = new jsonpathbuild();

        for (String path : paths) {
            int ln =  findLevel(path);
            for (int i = ln; i > 0; i--) {
                if(i  == ln ){
                    lastjs = getLastObject(path, lastjs,ln);
                }
                if(i  == ln-1 ){
                    lastjs2 = getLastObject2(path, lastjs2,ln-1);
                }
                if(i  == ln-2 ){
                    lastjs3 = getLastObject3(path, lastjs3,ln-2);
                }
                if(i  == ln-3 ){
                    lastjs4 = getLastObject4(path, lastjs4,ln-3);
                }
            }
        }
        System.out.println(lastjs);
        System.out.println(lastjs2);
        System.out.println(lastjs3);
        System.out.println(lastjs4);


        Map<String, JSONObject> firstlevel = new HashMap<String, JSONObject>();
       for(Map.Entry m1 : lastjs.entrySet()){
          String vl =  m1.getKey().toString();
           for(Map.Entry m2 : lastjs.entrySet()){
               if(m1 != m2){
                   if(StringUtils.substringBeforeLast(m1.getKey().toString(), ".").equalsIgnoreCase(
                           StringUtils.substringBeforeLast(m1.getKey().toString(), "."))){
                       JSONObject js = new JSONObject();

                      // for(String key : JSONObject.getNames((JSONObject)m1))
                       //{
                         js.put(JSONObject.getNames((JSONObject)m1.getValue()).toString(), "");
                          js.put(JSONObject.getNames((JSONObject)m2.getValue()).toString(), "");
                       //}
                     firstlevel.put(StringUtils.substringBeforeLast(m1.getKey().toString(), "."),js);
                   }
               }
           }
        }
        System.out.println(firstlevel);

    }

    private String insertValue(String rawJson, String xpath) {

        // Configuration ensuring addition of new leaves without raising exception
        Configuration conf = Configuration.defaultConfiguration()
                /*.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL)
                .addOptions(Option.SUPPRESS_EXCEPTIONS)*/;
        DocumentContext documentContext = JsonPath.using(conf).parse(rawJson).put(JsonPath.compile(xpath),"test", new String());
        //documentContext.set(JsonPath.compile(xpath), "");

        //JsonPath.using(conf).parse(rawJson).add("claim.claimdetails.tpl[0].address.addressline", "")

        return documentContext.put("$", "age", "aaa").jsonString();

    }

    public static void buildArray(String args) {
            List<String> list = new ArrayList<String>();
            list.add("Raja");
            list.add("Jai");
            list.add("Adithya");
            JSONArray array = new JSONArray();
            for(int i = 0; i < list.size(); i++) {
                array.put(list.get(i));
            }
            JSONObject obj = new JSONObject();
            try {
                obj.put("Employee Names:", array);
            } catch(JSONException e) {
                e.printStackTrace();
            }
            System.out.println(obj.toString());
        }


    public static  Map<String,JSONObject> getLastObject(String path,Map<String,JSONObject> js , int level){
        //Map<String,JSONObject> js  = new HashMap<String, JSONObject>();
        JSONObject obj = new JSONObject();
        String n1 = StringUtils.substringAfterLast(path,".");
        obj.put(n1, "");
        js.put(path+"_"+level,obj);
        return js;
    }

    public static  Map<String,JSONObject> getLastObject2(String path,Map<String,JSONObject> js , int level){
        //Map<String,JSONObject> js  = new HashMap<String, JSONObject>();
        JSONObject obj = new JSONObject();
        String n1 = StringUtils.substringAfterLast(StringUtils.substringBeforeLast(path,"."), ".");;
        obj.put(n1, "");
        js.put(StringUtils.substringBeforeLast(path,".")+"_"+level,obj);
        return js;
    }

    public static  Map<String,JSONObject> getLastObject3(String path,Map<String,JSONObject> js , int level){
        //Map<String,JSONObject> js  = new HashMap<String, JSONObject>();
        JSONObject obj = new JSONObject();
        String n1 = StringUtils.substringAfterLast(StringUtils.substringBeforeLast(
                StringUtils.substringBeforeLast(path,"."), "."),".");
        obj.put(n1, "");
        js.put(StringUtils.substringBeforeLast(
                StringUtils.substringBeforeLast(path,"."), ".")+"_"+level,obj);
        return js;
    }

    public static  Map<String,JSONObject> getLastObject4(String path,Map<String,JSONObject> js , int level){
        //Map<String,JSONObject> js  = new HashMap<String, JSONObject>();
        JSONObject obj = new JSONObject();
        String n1 =   StringUtils.substringBeforeLast(StringUtils.substringBeforeLast(
                StringUtils.substringBeforeLast(path,"."), "."),".") ;
        obj.put(n1, "");
        js.put(n1+"_"+level,obj);
        return js;
    }

    public static int findLevel(String path){
       return path.split("\\.").length;
    }

}