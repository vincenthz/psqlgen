#![allow(dead_code)]

use sqlparser::tokenizer::{Token, Word};
use thiserror::Error;

#[derive(Clone)]
pub struct TokenParser<'a> {
    tokens: &'a [Token],
    current: usize,
}

#[derive(Error, Debug)]
pub enum TokenError {
    #[error("expecting word but got {0:?}")]
    ExpectingWordButGot(Token),
    #[error("expecting keyword '{0}' but got {1:?}")]
    ExpectingKeywordButGot(String, Word),
    #[error("expecting '(' but got {0:?}")]
    ExpectingLParen(Token),
    #[error("expecting ')' but got {0:?}")]
    ExpectingRParen(Token),
    #[error("expecting ',' but got {0:?}")]
    ExpectingComma(Token),
    #[error("unexpected token {0:?}, expecting {1}")]
    UnexpectedToken(Token, &'static str),
}

impl<'a> TokenParser<'a> {
    pub fn new(toks: &'a [Token]) -> Self {
        TokenParser {
            tokens: toks,
            current: 0,
        }
    }

    pub fn is_end(&self) -> bool {
        self.tokens.len() == self.current
    }

    pub fn peek_next(&self) -> &'a Token {
        &self.tokens[self.current]
    }

    pub fn skip(&mut self) -> () {
        self.current += 1;
    }

    pub fn next(&mut self) -> &'a Token {
        if self.current >= self.tokens.len() {
            panic!(
                "out of bound access {:?} accessing {}",
                self.tokens, self.current
            )
        }
        let tok = &self.tokens[self.current];
        self.current += 1;
        tok
    }

    pub fn next_word(&mut self) -> Result<&'a Word, TokenError> {
        match self.next() {
            Token::Word(w) => Ok(w),
            t => Err(TokenError::ExpectingWordButGot(t.clone())),
        }
    }

    pub fn eat_lparen(&mut self) -> Result<(), TokenError> {
        match self.next() {
            Token::LParen => Ok(()),
            t => Err(TokenError::ExpectingLParen(t.clone())),
        }
    }

    pub fn eat_rparen(&mut self) -> Result<(), TokenError> {
        match self.next() {
            Token::RParen => Ok(()),
            t => Err(TokenError::ExpectingRParen(t.clone())),
        }
    }

    pub fn eat_word(&mut self, s: &str) -> Result<(), TokenError> {
        match self.next_word() {
            Ok(w) if w.value.as_str() == s => Ok(()),
            Ok(w) => Err(TokenError::ExpectingKeywordButGot(s.to_string(), w.clone())),
            Err(e) => Err(e),
        }
    }

    pub fn comma(&mut self) -> Result<(), TokenError> {
        match self.next() {
            Token::Comma => Ok(()),
            t => Err(TokenError::ExpectingComma(t.clone())),
        }
    }

    pub fn is_next_comma(&mut self) -> Result<bool, TokenError> {
        match self.peek_next() {
            Token::Comma => Ok(true),
            _ => Ok(false),
        }
    }

    pub fn next_word_is(&mut self, s: &str) -> bool {
        let tok = self.peek_next();
        if let Token::Word(w) = tok {
            if w.value.as_str() == s {
                self.current += 1;
                true
            } else {
                false
            }
        } else {
            false
        }
    }
}
