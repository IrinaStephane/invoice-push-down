public class Main {

    public static void main(String[] args) {

        DataRetriever dataRetriever = new DataRetriever();

        // =========================
        // Q1 - Total par facture
        // =========================
        System.out.println("=== Q1 - TOTAL PAR FACTURE ===");

        dataRetriever.findInvoiceTotals()
                .forEach(i ->
                        System.out.println(
                                i.getId() + " | " +
                                        i.getCustomerName() + " | " +
                                        i.getTotal()
                        )
                );

        // =========================
        // Q2 - CONFIRMED & PAID
        // =========================
        System.out.println("\n=== Q2 - FACTURES CONFIRMED ET PAID ===");

        dataRetriever.findConfirmedAndPaidInvoiceTotals()
                .forEach(i ->
                        System.out.println(
                                i.getId() + " | " +
                                        i.getCustomerName() + " | " +
                                        i.getStatus() + " | " +
                                        i.getTotal()
                        )
                );

        // =========================
        // Q3 - Totaux cumulés
        // =========================
        System.out.println("\n=== Q3 - TOTAUX PAR STATUT ===");

        InvoiceStatusTotal totals = dataRetriever.computeStatusTotals();

        System.out.println("total_paid = " + totals.getTotalPaid());
        System.out.println("total_confirmed = " + totals.getTotalConfirmed());
        System.out.println("total_draft = " + totals.getTotalDraft());

        // =========================
        // Q4 - CA pondéré
        // =========================
        System.out.println("\n=== Q4 - CHIFFRE D’AFFAIRES PONDÉRÉ ===");

        Double weightedTurnover = dataRetriever.computeWeightedTurnover();
        System.out.println(weightedTurnover);


        // =========================
        // Q5-A - Totaux HT / TVA / TTC
        // =========================
        System.out.println("\n=== Q5-A - TOTAUX HT / TVA / TTC PAR FACTURE ===");

        dataRetriever.findInvoiceTaxSummaries()
                .forEach(s ->
                        System.out.println(
                                s.getInvoiceId() +
                                        " | HT " + s.getTotalHt() +
                                        " | TVA " + s.getTotalTva() +
                                        " | TTC " + s.getTotalTtc()
                        )
                );

        // =========================
        // Q5-B - CA TTC pondéré
        // =========================
        System.out.println("\n=== Q5-B - CHIFFRE D’AFFAIRES TTC PONDÉRÉ ===");

        System.out.println(dataRetriever.computeWeightedTurnoverTtc());

    }
}
