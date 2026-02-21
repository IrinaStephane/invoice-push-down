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
}