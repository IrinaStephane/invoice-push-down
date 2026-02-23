import java.math.BigDecimal;
import java.util.List;

public class Main {

    public static void main(String[] args) {

        DataRetriever dataRetriever = new DataRetriever();

        // 🔹 Test 1 : Totaux HT / TVA / TTC par facture
        System.out.println("=== Invoice Tax Summaries ===");
        List<InvoiceTaxSummary> summaries = dataRetriever.findInvoiceTaxSummaries();

        for (InvoiceTaxSummary summary : summaries) {
            System.out.println(summary);
        }

        // 🔹 Test 2 : Chiffre d'affaires TTC pondéré
        System.out.println("\n=== Weighted Turnover TTC ===");
        BigDecimal weightedTurnover = dataRetriever.computeWeightedTurnoverTtc();
        System.out.println("Weighted Turnover TTC = " + weightedTurnover);
    }
}