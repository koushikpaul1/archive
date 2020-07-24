package com.leadics.poll.dto;
// Generated Sep 3, 2018 10:00:04 PM by Hibernate Tools 5.3.1.Final

import java.util.Date;

import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;

/**
 * Vote generated by hbm2java
 */
public class Vote implements java.io.Serializable {

	private Integer id;
	@ManyToOne
	@JoinColumn(name="q_id")
	private Questions questions;
	private String user;
	private String ip;
	private String country;
	private String state;
	private String city;
	private String question;
	private String answer;
	private Date votedAt;

	public Vote() {
	}

	public Vote(Questions questions) {
		this.questions = questions;
	}

	public Vote(Questions questions, String user, String ip, String country, String state, String city, String question,
			String answer, Date votedAt) {
		this.questions = questions;
		this.user = user;
		this.ip = ip;
		this.country = country;
		this.state = state;
		this.city = city;
		this.question = question;
		this.answer = answer;
		this.votedAt = votedAt;
	}

	public Integer getId() {
		return this.id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public Questions getQuestions() {
		return this.questions;
	}

	public void setQuestions(Questions questions) {
		this.questions = questions;
	}

	public String getUser() {
		return this.user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getIp() {
		return this.ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getCountry() {
		return this.country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getState() {
		return this.state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getCity() {
		return this.city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getQuestion() {
		return this.question;
	}

	public void setQuestion(String question) {
		this.question = question;
	}

	public String getAnswer() {
		return this.answer;
	}

	public void setAnswer(String answer) {
		this.answer = answer;
	}

	public Date getVotedAt() {
		return this.votedAt;
	}

	public void setVotedAt(Date votedAt) {
		this.votedAt = votedAt;
	}

}