import fs from 'node:fs'
import fsPromises from 'node:fs/promises'
import fetch from 'node-fetch'

import { FIL_WALLET_ADDRESS, LOG_INGESTOR_URL, nodeId, nodeToken, TESTING_CID ,INFLUXDB_ADDR } from '../config.js'
import { debug as Debug } from '../utils/logging.js'
import Influx from 'influxdb-nodejs'
const debug = Debug.extend('log-ingestor')

const client = new Influx('http://'+INFLUXDB_ADDR+'/saturn')
const fieldSchema = {
  addr: 'string',
  b: 'integer',
  lt: 'string',
  r: 'string',
  ref: 'string',
  rid: 'string',
  rt: 'string',
  s: 'string',
  ua: 'string',
  ucs: ['string']
}
const tagSchema = {
  spdy: ['speedy', 'fast', 'slow'],
  method: '*',
  type: [1, 2, 3, 4, 5]
}
client.schema('', fieldSchema, tagSchema, {
  stripUnknown: true
})
const NGINX_LOG_KEYS_MAP = {
  addr: 'clientAddress',
  b: 'numBytesSent',
  lt: 'localTime',
  r: 'request',
  ref: 'referrer',
  rid: 'requestId',
  rt: 'requestDuration',
  s: 'status',
  ua: 'userAgent',
  ucs: 'cacheHit'
}

let pending = []
let fh, hasRead
let parseLogsTimer
let submitRetrievalsTimer

export async function initLogIngestor () {
  if (fs.existsSync('/var/log/nginx/node-access.log')) {
    debug('Reading nginx log file')
    fh = await openFileHandle()

    parseLogs()

    submitRetrievals()
  }
}

async function parseLogs () {
  clearTimeout(parseLogsTimer)
  const read = await fh.readFile()

  if (read.length > 0) {
    hasRead = true
    const lines = read.toString().trim().split('\n')

    let valid = 0
    let hits = 0
    for (const line of lines) {
      const vars = line.split('&&').reduce((varsAgg, currentValue) => {
        const [name, ...value] = currentValue.split('=')
        const jointValue = value.join('=')

        let parsedValue
        switch (name) {
          case 'args': {
            parsedValue = jointValue.split('&').reduce((argsAgg, current) => {
              const [name, ...value] = current.split('=')
              return Object.assign(argsAgg, { [name]: value.join('=') })
            }, {})
            break
          }
          case 'lt':
          case 'rid':
          case 'addr': {
            parsedValue = jointValue
            break
          }
          case 'ucs': {
            parsedValue = jointValue === 'HIT'
            break
          }
          default: {
            const numberValue = Number.parseFloat(jointValue)
            parsedValue = Number.isNaN(numberValue) ? jointValue : numberValue
          }
        }
        return Object.assign(varsAgg, { [NGINX_LOG_KEYS_MAP[name] || name]: parsedValue })
      }, {})

      if (vars.request?.startsWith('/ipfs/') && vars.status === 200) {
        const { clientAddress, numBytesSent, request, requestId, localTime, requestDuration, args, range, cacheHit, referrer, userAgent } = vars
        const cidPath = request.replace('/ipfs/', '')
        const [cid, ...rest] = cidPath.split('/')
        const filePath = rest.join('/')

        if (cid === TESTING_CID) continue
        const { clientId } = args

        pending.push({
          cacheHit,
          cid,
          filePath,
          clientAddress,
          clientId,
          localTime,
          numBytesSent,
          range,
          referrer,
          requestDuration,
          requestId,
          userAgent
        })
        valid++
        if (cacheHit) hits++
      }
    }
    if (valid > 0) {
      debug(`Parsed ${valid} valid retrievals with hit rate of ${(hits / valid * 100).toFixed(0)}%`)
    }
  } else {
    if (hasRead) {
      await fh.truncate()
      await fh.close()
      hasRead = false
      fh = await openFileHandle()
    }
  }
  parseLogsTimer = setTimeout(parseLogs, 10_000)
}

async function openFileHandle () {
  return await fsPromises.open('/var/log/nginx/node-access.log', 'r+')
}

export async function submitRetrievals () {
  clearTimeout(submitRetrievalsTimer)
  if (pending.length > 0) {
    const body = {
      nodeId,
      filAddress: FIL_WALLET_ADDRESS,
      bandwidthLogs: pending
    }
    pending.forEach((item, index) => {
      client.write('http')
        .tag({ spdy: nodeId, method: 'GET', type: 1 })
        .field({
          addr: item.clientAddress,
          b: item.numBytesSent,
          lt: item.localTime,
          r: item.request,
          ref: item.referrer,
          rid: item.requestId,
          rt: item.requestDuration,
          s: item.status,
          ua: item.userAgent,
          ucs: item.cacheHit
        })
        .then(() => {
          debug('write point success') // eslint-disable-line no-console
        })
        .catch(err => {
          debug(err) // eslint-disable-line no-console
        })
    })

    try {
      await fetch(LOG_INGESTOR_URL, {
        method: 'POST',
        body: JSON.stringify(body),
        headers: {
          Authentication: nodeToken,
          'Content-Type': 'application/json'
        }
      })
      debug(`Submitted pending ${pending.length} retrievals to wallet ${FIL_WALLET_ADDRESS}`)
      pending = []
    } catch (err) {
      debug(`Failed to submit pending retrievals ${err.name} ${err.message}`)
    }
  }
  submitRetrievalsTimer = setTimeout(submitRetrievals, 60_000)
}
