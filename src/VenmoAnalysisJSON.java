// example of program that calculates the  median degree of a 
// venmo transaction graph

/**
 * @author Vagish Shanmukh
 * DataFormat - {"created_time": "2016-03-28T23:23:12Z", "target": "Raffi-Antilian", "actor": "Amber-Sauer"}
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class VenmoAnalysisJSON {

    public static void main(String[] args) {
        String inputFilePath = "C:\\coding-challenge\\data-gen\\venmo-trans.txt";
        String outputFilePath = "C:\\coding-challenge\\venmo_output\\output.txt";
        BufferedReader brIn = null;
        FileOutputStream fop = null;
        File ouputFile;
        //Active Transactions limit in seconds
        int secondsLimit = 60;
        try {
            brIn = new BufferedReader(new FileReader(inputFilePath));
            ouputFile = new File(outputFilePath);
            // if file doesnt exists, then create it
            if (!ouputFile.exists()) {
                ouputFile.createNewFile();
            }
            fop = new FileOutputStream(ouputFile);
            parseVenmoAnalysisJsonFile(brIn, fop, secondsLimit);
            //            mergeMedian(mapTransactions);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            try {
                if (brIn != null){
                    brIn.close();
                }
                if (fop != null){
                    fop.flush();
                    fop.close();
                }

            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }


    /**
     * parse the JSON file and put the data into a hashmap
     * @param brIn
     * @param fop
     * @param secondsLimit
     * @throws Exception
     */
    public static void parseVenmoAnalysisJsonFile(BufferedReader brIn, FileOutputStream fop, int secondsLimit) throws Exception {

        JSONParser parser = new JSONParser();
        //        String created_time = null;
        //        String target = null;
        //        String actor = null;
        Transaction currentTransaction = null;
        //        HashMap<String, ArrayList<Transaction>> mapTransactions = new HashMap<String, ArrayList<Transaction>>();
        Object obj;
        List<Transaction> activeTransactions = new ArrayList<Transaction>();
        Map<String, Integer> mapEdge = new HashMap<String, Integer>();
        int edge = 0;

        long diff = 0;
        List<Transaction> removalActiveTransactions = new ArrayList<Transaction>();
        try {
            String sCurrentLine;
            while ((sCurrentLine = brIn.readLine()) != null) {
                //                System.out.println("Record:\t" + sCurrentLine);
                try {
                    obj = parser.parse(sCurrentLine);
                    //JSONObject jsonObject = (JSONObject) obj;

                    //                    created_time = (String) ((org.json.simple.JSONObject) obj).get("created_time");
                    //                    System.out.println(created_time + "            " + JSONTarihConvert(created_time));

                    //                    target = (String) ((org.json.simple.JSONObject) obj).get("target");
                    //                    System.out.println(target);

                    //                    actor = (String) ((org.json.simple.JSONObject) obj).get("actor");
                    //                    System.out.println(actor);
                    // Current Transaction object
                    currentTransaction = new Transaction(JSONTarihConvert((String) ((org.json.simple.JSONObject) obj).get("created_time")),
                            (String) ((org.json.simple.JSONObject) obj).get("target"),
                            (String) ((org.json.simple.JSONObject) obj).get("actor"));

                    // First Transaction object added to active list
                    if (null != activeTransactions && activeTransactions.size() == 0) {
                        activeTransactions.add(currentTransaction);
                        mapEdge.put(currentTransaction.getActor(), 1);
                        mapEdge.put(currentTransaction.getTarget(), 1);

                    } else {
                        for (int i = 0; i < activeTransactions.size(); i++) {
                            Transaction removeTransaction = activeTransactions.get(i);

                            diff = (currentTransaction.getCreated_time().getTime() - removeTransaction.getCreated_time().getTime()) / 1000;

                            // getting list of active transaction objects for 60 seconds
                            if (diff > secondsLimit) {
                                // Adding to another list which will be used to remove the objects
                                removalActiveTransactions.add(removeTransaction);
                            }
                        }
                        activeTransactions.add(currentTransaction);

                        if (mapEdge.containsKey(currentTransaction.getActor())) {
                            edge = mapEdge.get(currentTransaction.getActor()).intValue();
                            mapEdge.put(currentTransaction.getActor(), ++edge);

                        } else {
                            mapEdge.put(currentTransaction.getActor(), 1);

                        }

                        if (mapEdge.containsKey(currentTransaction.getTarget())) {
                            edge = mapEdge.get(currentTransaction.getTarget()).intValue();
                            mapEdge.put(currentTransaction.getTarget(), ++edge);

                        } else {
                            mapEdge.put(currentTransaction.getTarget(), 1);

                        }

                    }
                    // Removing transaction which are more than 60 seconds 
                    for (int i = 0; i < removalActiveTransactions.size(); i++) {
                        Transaction transaction = removalActiveTransactions.get(i);
                        //Checking the data for Actor in the mapEdge
                        edge = mapEdge.get(transaction.getActor());

                        if (edge == 1 || edge == 0) {

                            mapEdge.remove(transaction.getActor());

                        } else {

                            mapEdge.put(transaction.getActor(), --edge);

                        }

                        //Checking the data for Target in the mapEdge
                        edge = mapEdge.get(transaction.getTarget());

                        if (edge == 1 || edge == 0) {

                            mapEdge.remove(transaction.getTarget());

                        } else {

                            mapEdge.put(transaction.getTarget(), --edge);

                        }

                        activeTransactions.remove(transaction);

                    }
                    removalActiveTransactions.clear();

                    // List of active transactions for 60 seconds
                    int j = 0;
//                    for (Iterator iterator = activeTransactions.iterator(); iterator.hasNext();) {
//                        Transaction transaction = (Transaction) iterator.next();
//                        System.out.println("ACTIVE::::: " + j++ + "    " + transaction.getActor() + "         " + transaction.getTarget()
//                                + "         " + transaction.getCreated_time());
//
//                    }
                    String key = null;

                    // sorting the values in ascending order
                    mapEdge = sortByValue(mapEdge);

                    //Printing the map & median
                    //for (Map.Entry<String, Integer> entry : mapEdge.entrySet()) {
                    //key = entry.getKey();
                    //edge = entry.getValue();
                    //System.out.println("KEY:::: " + key + "     VALUE:::: " + edge);
                    //
                    //}
                    fop.write(((Double.toString(calculateMedian(mapEdge)) + "\n").getBytes()));

                    //System.out.println("Median:::: " + calculateMedian(mapEdge));

                    //                    mapNode.

                } catch (ParseException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

            //            System.out.println("Linked list size ::: " + activeTransactions.size());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /** Calculation of median from the hashmap data
     * @param mapEdge
     * @return
     */
    private static double calculateMedian(Map<String, Integer> mapEdge) {
        Object[] edges = mapEdge.values().toArray();

        int middle = edges.length / 2;
        if (edges.length % 2 == 1) {
            return ((Integer) edges[middle]).doubleValue();
        } else {
            return (((Integer) edges[middle - 1]).doubleValue() + ((Integer) edges[middle]).doubleValue()) / 2.0;
        }
    }

    /**
     * Conversion of JSON String Timestamp to java.sql.Timestamp
     * @param tarih
     * @return Timestamp
     * @throws java.text.ParseException
     */
    private static Timestamp JSONTarihConvert(String tarih) throws java.text.ParseException {

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssz");

        if (tarih.endsWith("Z")) {
            tarih = tarih.substring(0, tarih.length() - 1) + "GMT-00:00";
        } else {
            int inset = 6;

            String s0 = tarih.substring(6, tarih.length() - 1 - inset);
            String s1 = tarih.substring(tarih.length() - inset, tarih.length() - 2);

            tarih = s0 + "GMT" + s1;
        }

        return new Timestamp(df.parse(tarih).getTime());

    }


    /**
     * Sorting the values of HashMap in ascending order.
     * @param map
     * @return map
     */
    private static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return (o1.getValue()).compareTo(o2.getValue());
            }
        });

        Map<K, V> result = new LinkedHashMap<K, V>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }
}
