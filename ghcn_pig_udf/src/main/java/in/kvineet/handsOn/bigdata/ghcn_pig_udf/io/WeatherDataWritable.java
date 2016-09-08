/**
 * 
 */
package in.kvineet.bigdata.ghcn_pig_udf.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * @author CoolKarni
 *
 */
public class WeatherDataWritable implements WritableComparable<WeatherDataWritable>, Cloneable {

	private String ID;
	private String date;
	private String element;
	private int value;
	private char mflag;
	private char qflag;
	private char sflag;

	public static WeatherDataWritable readWeatherData(DataInput in) throws IOException{
		WeatherDataWritable wd = new WeatherDataWritable();
		wd.readFields(in);
		return wd;
		
	}
	public void readFields(DataInput in) throws IOException {
		ID  = Text.readString(in);
		date = Text.readString(in);
		element = Text.readString(in);
		value = WritableUtils.readVInt(in);
		String flags = Text.readString(in);
		mflag = flags.charAt(0);
		qflag = flags.charAt(1);
		sflag = flags.charAt(2);
	}
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, ID);
		Text.writeString(out, date);
		Text.writeString(out, element);
		WritableUtils.writeVInt(out, value);
		String flags = ""+mflag+qflag+sflag;
		Text.writeString(out, flags);
	}
	
	public int compareTo(WeatherDataWritable o) {
		return this.value < o.value ? -1 : (this.value == o.value ? 0 : 1);
	}
	
	@Override
	public WeatherDataWritable clone() throws CloneNotSupportedException {
		return (WeatherDataWritable) super.clone();
	}
	
	/* auto generated getter setter methods*/
	public String getID() {
		return ID;
	}
	public void setID(String iD) {
		ID = iD;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public String getElement() {
		return element;
	}
	public void setElement(String element) {
		this.element = element;
	}
	public int getValue() {
		return value;
	}
	public void setValue(int value) {
		this.value = value;
	}
	public char getMflag() {
		return mflag;
	}
	public void setMflag(char mflag) {
		this.mflag = mflag;
	}
	public char getQflag() {
		return qflag;
	}
	public void setQflag(char qflag) {
		this.qflag = qflag;
	}
	public char getSflag() {
		return sflag;
	}
	public void setSflag(char sflag) {
		this.sflag = sflag;
	}
	public WeatherDataWritable(String iD, String date, Byte month, String element, Integer value,
			char mflag, char qflag, char sflag) {
		this.ID = iD;
		this.date = date;
		this.element = element;
		this.value = value;
		this.mflag = mflag;
		this.qflag = qflag;
		this.sflag = sflag;
	}
	
	public WeatherDataWritable(){
	}
	@Override
	public String toString() {
		return ID+date+element+String.format("%05d", value)+mflag+qflag+sflag;
	}
	
	
}
