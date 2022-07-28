import fs from 'node:fs'
import fsPromises from 'node:fs/promises'
import fetch from 'node-fetch'
import prettyBytes from 'pretty-bytes'

import { FIL_WALLET_ADDRESS, LOG_INGESTOR_URL, nodeId, nodeToken, TESTING_CID, INFLUXDB_ADDR } from '../config.js'
import { debug as Debug } from '../utils/logging.js'

const debug = Debug.extend('log-ingestor')

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

const ONE_GIGABYTE = 1073741823

let pending = []
let fh, hasRead
let parseLogsTimer
let submitRetrievalsTimer

export async function initLogIngestor () {
  debug('abcd')
  if (fs.existsSync('/var/log/nginx/node-access.log')) {
    debug('Reading nginx log file')
    fh = await openFileHandle()
debug('abcdef')
    parseLogs()

    submitRetrievals()
  }
}

async function parseLogs () {
  clearTimeout(parseLogsTimer)
  const stat = await fh.stat()

  if (stat.size > ONE_GIGABYTE) {
    // Got to big we can't read it into single string
    // TODO: stream read it
    await fh.truncate()
  }

  const read = await fh.readFile()

  let valid = 0
  let hits = 0
  if (read.length > 0) {
    hasRead = true
    const lines = read.toString().trim().split('\n')

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
      debug(`Parsed ${valid} valid retrievals in ${prettyBytes(read.length)} with hit rate of ${Number((hits / valid * 100).toFixed(2))}%`)
    }
  } else {
    if (hasRead) {
      await fh.truncate()
      await fh.close()
      hasRead = false
      fh = await openFileHandle()
    }
  }
  parseLogsTimer = setTimeout(parseLogs, Math.max(10_000 - valid, 1000))
}

async function openFileHandle () {
  return await fsPromises.open('/var/log/nginx/node-access.log', 'r+')
}

export async function submitRetrievals () {
  clearTimeout(submitRetrievalsTimer)
  const length = pending.length
  debug(pending, 222)
  if (length > 0) {
    const body = {
      nodeId,
      filAddress: FIL_WALLET_ADDRESS,
      bandwidthLogs: pending
    }
    let dataString = ''
    const tagString = `net,nodeId=${nodeId}`
    pending.forEach((item, index) => {
      const time = (Date.parse(item.localTime) * 1000 * 1000) + index * 1000
      // debug(item.localTime, time, '2222')

      dataString = dataString + ` ${tagString} clientAddress="${item.clientAddress}",numBytesSent=${item.numBytesSent},request="${item.request}",referrer="${item.referrer}",rid="${item.requestId}",requestDuration=${item.requestDuration},status="${item.status}",cacheHit="${item.cacheHit}" ${time}\n`
    })
    // debug(`aaaa ${dataString}`)
    try {
      fetch('http://' + INFLUXDB_ADDR + '/write?db=saturn', {
        method: 'POST',
        body: dataString,
        headers: {
          'Content-Type': 'text/plain'
        }
      }).catch(error => {
        debug(error)
      })
      debug('write points success' + INFLUXDB_ADDR)
    } catch (err) {
      debug(`Failed write points success ${err.name} ${err.message}`)
    }
    pending = []
    debug(body, 333)
    try {
      debug(`Submitting ${length} pending retrievals`)
      const startTime = Date.now()
      await fetch(LOG_INGESTOR_URL, {
        method: 'POST',
        body: JSON.stringify(body),
        headers: {
          Authentication: nodeToken,
          'Content-Type': 'application/json'
        }
      })
      debug(`Submitted ${length} retrievals to wallet ${FIL_WALLET_ADDRESS} in ${Date.now() - startTime}ms`)
    } catch (err) {
      debug(`Failed to submit pending retrievals ${err.name} ${err.message}`)
      pending = body.bandwidthLogs.concat(pending)
    }
  }
  submitRetrievalsTimer = setTimeout(submitRetrievals, Math.max(60_000 - length, 10_000))
}
