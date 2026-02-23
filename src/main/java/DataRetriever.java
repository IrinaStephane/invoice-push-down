import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DataRetriever {
    DBConnection dbConnection = new DBConnection();

    List<InvoiceTotal> findInvoiceTotals() {
        List<InvoiceTotal> list = new ArrayList<>();
        try (Connection connection = dbConnection.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(
                     """
                             select invoice_id, customer_name, sum(quantity * unit_price) as total_amount from invoice_line
                                 join invoice i on i.id = invoice_line.invoice_id group by invoice_id, customer_name ;
                             """);
             ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                InvoiceTotal invoiceTotal = new InvoiceTotal();
                invoiceTotal.setId(resultSet.getInt("invoice_id"));
                invoiceTotal.setCustomerName(resultSet.getString("customer_name"));
                invoiceTotal.setTotal(resultSet.getDouble("total_amount"));
                list.add(invoiceTotal);
            }
            return list;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    List<InvoiceTotal> findConfirmedAndPaidInvoiceTotals() {
        List<InvoiceTotal> list = new ArrayList<>();
        try (Connection connection = dbConnection.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(
                     """
                                    select invoice_id, customer_name, sum(quantity * unit_price) as total_amount, status
                                    from invoice_line
                                        join invoice i on i.id = invoice_line.invoice_id
                                    where status = 'CONFIRMED' or status = 'PAID' group by invoice_id, customer_name, status;
                             """);
             ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                InvoiceTotal invoiceTotal = new InvoiceTotal();
                invoiceTotal.setId(resultSet.getInt("invoice_id"));
                invoiceTotal.setCustomerName(resultSet.getString("customer_name"));
                invoiceTotal.setTotal(resultSet.getDouble("total_amount"));
                invoiceTotal.setStatus(InvoiceStatus.valueOf(resultSet.getString("status")));
                list.add(invoiceTotal);
            }
            return list;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    InvoiceStatusTotal computeStatusTotal() {
        try (Connection connection = dbConnection.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(
                     """
                                   select sum((case when i.status = 'PAID' then il.quantity * il.unit_price else 0 end))      as paid_amount,
                                          sum((case when i.status = 'CONFIRMED' then il.quantity * il.unit_price else 0 end)) as confirmed_amount,
                                          sum((case when i.status = 'DRAFT' then il.quantity * il.unit_price else 0 end))     as draft_amount
                                   from invoice i
                                            join public.invoice_line il on i.id = il.invoice_id;
                             """);
             ResultSet resultSet = preparedStatement.executeQuery()) {
            if (resultSet.next()) {
                InvoiceStatusTotal invoiceStatusTotal = new InvoiceStatusTotal();
                invoiceStatusTotal.setInvoicePaid(resultSet.getDouble("paid_amount"));
                invoiceStatusTotal.setInvoiceConfirmed(resultSet.getDouble("confirmed_amount"));
                invoiceStatusTotal.setInvoiceDraft(resultSet.getDouble("draft_amount"));
                return invoiceStatusTotal;
            }
            throw new RuntimeException("Unable to compute invoice status total");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Double computeWeightTurnOver() {
        try (Connection connection = dbConnection.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(
                     """
                                select sum(case
                                               when i.status = 'PAID' then quantity * unit_price * 1.0
                                               else
                                                   case
                                                       when i.status = 'CONFIRMED' then quantity * unit_price * 0.5
                                                       else
                                                           case
                                                               when i.status = 'DRAFT' then quantity * unit_price * 0
                                                               else 0 end
                                                       end
                                    end) as revenue_percent
                                from invoice_line il
                                         join invoice i on i.id = il.invoice_id
                             """);
             ResultSet resultSet = preparedStatement.executeQuery()) {
            if (resultSet.next()) {
                return resultSet.getDouble("revenue_percent");
            }
            throw new RuntimeException("Unable to compute weight turnover");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    List<InvoiceTaxSummary> findInvoiceTaxSummaries() {
        List<InvoiceTaxSummary> list = new ArrayList<>();

        try (Connection connection = dbConnection.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(
                     """
                     select i.id as invoice_id,
                            sum(il.quantity * il.unit_price) as total_ht,
                            sum(il.quantity * il.unit_price) * t.rate / 100 as total_tva,
                            sum(il.quantity * il.unit_price) +
                            (sum(il.quantity * il.unit_price) * t.rate / 100) as total_ttc
                     from invoice i
                              join invoice_line il on i.id = il.invoice_id,
                          tax_config t
                     group by i.id, t.rate
                     order by i.id;
                     """);
             ResultSet resultSet = preparedStatement.executeQuery()) {

            while (resultSet.next()) {
                InvoiceTaxSummary summary = new InvoiceTaxSummary();
                summary.setInvoiceId(resultSet.getInt("invoice_id"));
                summary.setTotalHt(resultSet.getDouble("total_ht"));
                summary.setTotalTva(resultSet.getDouble("total_tva"));
                summary.setTotalTtc(resultSet.getDouble("total_ttc"));
                list.add(summary);
            }

            return list;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public BigDecimal computeWeightedTurnoverTtc() {

        try (Connection connection = dbConnection.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(
                     """
                     select sum(
                             case
                                 when i.status = 'PAID' then ttc * 1.0
                                 when i.status = 'CONFIRMED' then ttc * 0.5
                                 when i.status = 'DRAFT' then 0
                                 else 0
                             end
                         ) as revenue_percent_ttc
                     from (
                              select i.id,
                                     i.status,
                                     sum(il.quantity * il.unit_price) +
                                     (sum(il.quantity * il.unit_price) * t.rate / 100) as ttc
                              from invoice i
                                       join invoice_line il on i.id = il.invoice_id,
                                   tax_config t
                              group by i.id, i.status, t.rate
                          ) invoice_ttc
                              join invoice i on invoice_ttc.id = i.id;
                     """);
             ResultSet resultSet = preparedStatement.executeQuery()) {

            if (resultSet.next()) {
                return resultSet.getBigDecimal("revenue_percent_ttc");
            }

            throw new RuntimeException("Unable to compute weighted turnover TTC");

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}