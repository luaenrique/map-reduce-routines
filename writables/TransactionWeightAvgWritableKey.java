package advanced.transacoes.writables;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class TransactionWeightAvgWritableKey implements WritableComparable<TransactionWeightAvgWritableKey> {
    private String year;
    private String commodity;


    public TransactionWeightAvgWritableKey(){}

    public TransactionWeightAvgWritableKey(String commodity, String year) {
        this.year = year;
        this.commodity = commodity;
    }

    public String getYear() {
        return year;
    }

    public String getCommodity(){
        return commodity;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public void setCommodity(String commodity) {
        this.commodity = commodity;
    }





    //metodo de leitura
    @Override
    public void readFields(DataInput in) throws IOException {
        this.commodity      = in.readUTF();
        this.year           = in.readUTF();
    }


    //metodo de escrita
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(commodity));
        out.writeUTF(String.valueOf(year));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionWeightAvgWritableKey that = (TransactionWeightAvgWritableKey) o;
        return year.equals(that.year) &&
                commodity.equals(that.commodity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(year, commodity);
    }


    //comparator para as customKeys
    @Override
    public int compareTo(TransactionWeightAvgWritableKey t) {
        String thiskey1 = this.commodity;
        String thatkey1 = t.commodity;

        String thiskey2 = this.year;
        String thatkey2 = t.year;

        int cmp = thiskey1.compareTo(thatkey1);
        if (cmp != 0) {
            return cmp;
        }
        return thiskey2.compareTo(thatkey2);
    }
}
