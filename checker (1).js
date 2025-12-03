import express from express;
import { TelegramClient } from telegram;
import { StringSession } from telegram/sessions/index.js;
import { Api } from telegram;

/** --- simple auth --- */
function isAuth(req) {
  const h = req.headers[authorization] || ;
  const token = h.startsWith(Bearer