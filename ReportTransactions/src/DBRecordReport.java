

import java.io.Serializable;


public class DBRecordReport implements Serializable {
	private String intchgIndividual;
	private String sender;
	private String receiver;
	private String calculatedNumber;
	
	public String getIntchgIndividual() {
		return intchgIndividual;
	}
	public void setIntchgIndividual(String intchgIndividual) {
		this.intchgIndividual = intchgIndividual;
	}
	public String getSender() {
		return sender;
	}
	public void setSender(String sender) {
		this.sender = sender;
	}
	public String getReceiver() {
		return receiver;
	}
	public void setReceiver(String receiver) {
		this.receiver = receiver;
	}
	public String getCalculatedNumber() {
		return calculatedNumber;
	}
	public void setCalculatedNumber(String calculatedNumber) {
		this.calculatedNumber = calculatedNumber;
	}
}
