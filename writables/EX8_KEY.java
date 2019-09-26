package advanced.transacoes.writables;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class EX8_KEY implements WritableComparable<EX8_KEY> {
    private String year;
    private String flow;



    public EX8_KEY() {    }

    public EX8_KEY(String year, String flow) {
        this.year = year;
        this.flow = flow;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getFlow() {
        return flow;
    }

    public void setFlow(String flow) {
        this.flow = flow;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(year));
        out.writeUTF(String.valueOf(flow));
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year      = in.readUTF();
        this.flow      = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EX8_KEY ex8_key = (EX8_KEY) o;
        return year.equals(ex8_key.year) &&
                flow.equals(ex8_key.flow);
    }

    @Override
    public int hashCode() {
        return Objects.hash(year, flow);
    }

    public int compareTo(EX8_KEY t) {
        String thiskey1 = this.flow;
        String thatkey1 = t.flow;

        String thiskey2 = this.year;
        String thatkey2 = t.year;

        int cmp = thiskey1.compareTo(thatkey1);
        if (cmp != 0) {
            return cmp;
        }
        return thiskey2.compareTo(thatkey2);
    }

}
