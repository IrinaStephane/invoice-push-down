import java.sql.Connection;
import java.sql.SQLException;

public class Main {
    public static void main(String[] args) {
        DataRetriever dataRetriever = new DataRetriever();
        //System.out.println(dataRetriever.findConfirmedAndPaidInvoiceTotals());
        //System.out.println(dataRetriever.findInvoiceTotals());
        System.out.println(dataRetriever.computeWeightTurnOver());
    }
}