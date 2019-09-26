package advanced.transacoes.writables;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class EX7Writable implements Writable {
    private float valor;
    private float peso;

    public EX7Writable() {
    }

    public float getValor() {
        return valor;
    }

    public void setValor(float valor) {
        this.valor = valor;
    }

    public float getPeso() {
        return peso;
    }

    public void setPeso(float peso) {
        this.peso = peso;
    }

    /*public void set(float value, int n) {
        this.value = value;
        this.n = n;
    }/*

     */


    public EX7Writable(float valor, float peso) {
        this.valor = valor;
        this.peso = peso;
    }


    //reduce lê como do map? como o arquivo lê do reduce?
    @Override
    public void readFields(DataInput in) throws IOException {
        this.valor = Float.parseFloat(in.readUTF());
        this.peso = Float.parseFloat(in.readUTF());
    }


    //dados estão saindo do map e indo pro reduce ou do reduce para o arquivo
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(valor));
        out.writeUTF(String.valueOf(peso));
    }
}