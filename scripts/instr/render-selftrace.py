#!/usr/bin/env python3
"""Render Chrome Trace JSON as an offline, interactive HTML swimlane timeline."""

import argparse
from fnmatch import fnmatchcase
import gzip
import heapq
import html
import json
import math
from pathlib import Path
import sys


PRESETS = {
    "none": [],
    "gc": [
        ("* stack_size", {"render": "bars", "height": 54, "order": 200}),
        ("* recent_size", {"render": "bars", "height": 54, "order": 200}),
        ("* mbuf_size", {"render": "bars", "height": 54, "order": 200}),
    ],
}


def counter_height(value):
    height = int(value)
    if not 40 <= height <= 600:
        raise ValueError("Counter height must be between 40 and 600 pixels")
    return height


def style_rule(value):
    """Parse GLOB: render=line|bars mode=linear|step height=PIXELS order=INTEGER; properties are optional."""
    pattern, separator, declarations = value.rpartition(":")
    if not separator or not pattern.strip() or not declarations.strip():
        raise ValueError("Expected GLOB: render=line|bars mode=linear|step height=PIXELS order=INTEGER")
    properties = {}
    for declaration in declarations.split():
        key, equal, setting = declaration.partition("=")
        if key == "render" and equal and setting in ("line", "bars"):
            properties[key] = setting
        elif key == "mode" and equal and setting in ("linear", "step"):
            properties[key] = setting
        elif key == "height" and equal:
            properties[key] = counter_height(setting)
        elif key == "order" and equal:
            properties[key] = int(setting)
        else:
            raise ValueError(f"Invalid track style: {declaration}")
    return pattern.strip().casefold(), properties


def customize(data, preset="gc", height=66, rules=()):
    for track in data["tracks"]:
        track["order"] = 0
        if track["kind"] == "counter":
            track.update(render="line", mode="linear", height=counter_height(height), order=100)
        for pattern, properties in [*PRESETS[preset], *rules]:
            if fnmatchcase(track["name"].casefold(), pattern):
                track.update({key: value for key, value in properties.items()
                              if key == "order" or track["kind"] == "counter"})
    data["tracks"].sort(key=lambda t: (t["order"], t["name"].casefold(), t["pid"]))


def number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def prepare(document, filters=(), excludes=()):
    events = document.get("traceEvents", []) if isinstance(document, dict) else document
    if not isinstance(events, list):
        raise ValueError("Expected a traceEvents array or a top-level event array")
    names, processes, tracks, stacks = {}, {}, {}, {}
    warnings = []
    timed = []
    for index, event in enumerate(events):
        if not isinstance(event, dict):
            raise ValueError(f"Event {index} is not an object")
        pid, tid = str(event.get("pid", 0)), str(event.get("tid", 0))
        if event.get("ph") == "M":
            if event.get("name") == "thread_name":
                names[pid, tid] = str(event.get("args", {}).get("name", tid))
            elif event.get("name") == "process_name":
                processes[pid] = str(event.get("args", {}).get("name", pid))
        elif event.get("ph") in ("B", "E", "X", "C", "i", "I"):
            if not number(event.get("ts")):
                raise ValueError(f"Event {index} has an invalid timestamp")
            timed.append(event)
        else:
            warnings.append(f"Unsupported event phase: {event.get('ph')!r}")
    timed.sort(key=lambda e: e["ts"])
    origin = min((e["ts"] for e in timed), default=0)
    end = max((e["ts"] for e in timed), default=origin)

    def lane(pid, tid):
        key = ("slice", pid, tid)
        if key not in tracks:
            name = names.get((pid, tid), f"{processes.get(pid, pid)} / thread {tid}")
            tracks[key] = {"name": name, "kind": "slice", "items": [], "pid": pid, "tid": tid}
        return tracks[key]

    def interval(track, start, finish, incomplete=False, end_args=None):
        track["items"].append({
            "start": start["ts"] - origin, "end": finish - origin,
            "name": str(start.get("name", "(unnamed)")), "category": start.get("cat", ""),
            "args": start.get("args", {}), "endArgs": end_args or {}, "incomplete": incomplete,
        })

    for event in timed:
        pid, tid = str(event.get("pid", 0)), str(event.get("tid", 0))
        phase, timestamp = event["ph"], event["ts"]
        if phase == "C":
            for field, value in event.get("args", {}).items():
                if not number(value):
                    warnings.append("Ignored nonnumeric counter values")
                    continue
                # Chrome JSON counters are process-scoped; tid does not identify a series.
                key = ("counter", pid, str(event.get("cat", "")), str(event.get("name", "")), field)
                if key not in tracks:
                    tracks[key] = {"name": f"{event.get('name', 'counter')} {field}",
                                   "kind": "counter", "counterName": field, "items": [], "pid": pid}
                tracks[key]["items"].append([timestamp - origin, value])
        elif phase == "B":
            stacks.setdefault((pid, tid), []).append(event)
        elif phase == "E":
            stack = stacks.setdefault((pid, tid), [])
            if stack:
                interval(lane(pid, tid), stack.pop(), timestamp, end_args=event.get("args"))
            else:
                warnings.append("Ignored unmatched slice ends")
        elif phase == "X":
            duration = event.get("dur")
            if not number(duration) or duration < 0:
                raise ValueError("Complete events require a finite, nonnegative dur")
            end = max(end, timestamp + duration)
            interval(lane(pid, tid), event, timestamp + duration)
        else:
            interval(lane(pid, tid), event, timestamp)
    for (pid, tid), stack in stacks.items():
        for start in stack:
            interval(lane(pid, tid), start, end, incomplete=True)
            warnings.append("Open slices extend to the trace end and are marked incomplete")
    selected = []
    for track in tracks.values():
        if filters and not any(term.casefold() in track["name"].casefold() for term in filters):
            continue
        if any(term.casefold() in track["name"].casefold() for term in excludes):
            continue
        if track["kind"] == "slice":
            track["items"].sort(key=lambda item: (item["start"], -item["end"]))
            busy, free, depth = [], [], 0
            for item in track["items"]:
                while busy and busy[0][0] <= item["start"]:
                    _, row = heapq.heappop(busy)
                    heapq.heappush(free, row)
                if free:
                    row = heapq.heappop(free)
                else:
                    row = depth
                    depth += 1
                item["depth"] = row
                heapq.heappush(busy, (item["end"], row))
            track["depth"] = max(1, depth)
        selected.append(track)
    selected.sort(key=lambda t: (t["kind"] == "counter", t["name"].casefold(), t["pid"]))
    return {"tracks": selected, "duration": max(1, end - origin), "origin": origin,
            "warnings": sorted(set(warnings))}


TEMPLATE = r'''<!doctype html>
<html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>__TITLE__</title>
<style>
:root{color-scheme:dark;font:12px system-ui,sans-serif;background:#10151e;color:#dae3f1}
*{box-sizing:border-box}body{margin:0;height:100vh;display:flex;flex-direction:column}header{flex-shrink:0}header{padding:12px 16px;background:#192230;border-bottom:1px solid #354156}
h1{font-size:15px;margin:0;min-width:0;overflow-wrap:anywhere}.title-row{display:flex;align-items:center;justify-content:space-between;gap:16px;margin-bottom:7px}.title-row nav{flex-wrap:nowrap;flex-shrink:0}button,input{font:inherit;color:inherit;background:#253247;border:1px solid #4a5d78;border-radius:5px;padding:5px 9px}
button{cursor:pointer}button:hover{background:#354965}input{width:280px}nav{display:flex;gap:7px;align-items:center;flex-wrap:wrap}
#zoomIn,#zoomOut{width:29px;height:29px;padding:0;display:inline-grid;place-items:center;font-size:16px;line-height:1}
#trackCount{color:#aebed3;white-space:nowrap}#notice{color:#efbf73;white-space:pre-wrap}
#help{max-width:min(420px,90vw);padding:16px;background:#253247;color:#dae3f1;border:1px solid #7c96ba;border-radius:5px;line-height:1.8}
#viewer{flex:1;min-height:0;overflow:auto;position:relative;overscroll-behavior:contain}
.row{display:flex;border-bottom:1px solid #293345;min-width:550px}.row[hidden]{display:none}.label{width:260px;flex-shrink:0;padding:8px 11px;background:#18202c;overflow-wrap:anywhere}
.label small{display:block;color:#92a6bf;margin-top:4px}.plot{flex:1;min-width:290px;overflow:hidden}canvas{display:block;width:100%;touch-action:none;cursor:grab}
#ruler{position:sticky;top:0;z-index:2;background:#10151e}#ruler .label{padding:4px 12px}
#ruler #timeRange{padding:4px 12px;font-size:11px;line-height:16px;white-space:nowrap;font-variant-numeric:tabular-nums}
#tip{position:fixed;display:none;pointer-events:none;background:#253247;border:1px solid #7c96ba;border-radius:5px;padding:8px;max-width:min(720px,95vw);max-height:80vh;overflow:auto;overflow-wrap:anywhere;white-space:pre-wrap;z-index:4}
.counter-tip{position:fixed;display:none;pointer-events:none;background:#253247;border:1px solid #7c96ba;border-radius:4px;padding:3px 6px;font-variant-numeric:tabular-nums;z-index:4}
</style>
<header><div class="title-row"><h1>__TITLE__</h1><nav aria-label="Timeline controls">
<button id="fit">Fit all</button><button id="zoomIn" aria-label="Zoom in">+</button><button id="zoomOut" aria-label="Zoom out">−</button>
<button popovertarget="help" aria-label="Timeline help" title="Timeline help">?</button></nav></div>
<nav>__FILTER__
__EXCLUDE__
__TRACK_COUNT__
</nav>
<div id="notice"></div></header>
<div id="help" popover>Hover for details · Escape to dismiss · drag to pan · scroll to navigate · Ctrl/⌘+wheel to zoom</div>
<div id="viewer" tabindex="0" aria-label="Timeline: scroll vertically through tracks, scroll horizontally to pan, Control or Command and wheel to zoom, drag to pan">
<div class="row" id="ruler"><div class="label" id="timeRange" aria-label="Visible time range and span"></div><div class="plot"><canvas height="24"></canvas></div></div><div id="tracks"></div></div>
<div id="tip"></div><script id="trace-data" type="application/json">__DATA__</script>
<script>
'use strict';
const data=JSON.parse(document.getElementById('trace-data').textContent);
const $=id=>document.getElementById(id), rows=[], tip=$('tip');
let lo=0,hi=data.duration,drag=null,pending=false,hoverTime=null;
$('notice').textContent=data.warnings.join('\n');
const time=n=>Math.abs(n)>=1e6?(n/1e6).toFixed(4)+' s':Math.abs(n)>=1000?(n/1000).toFixed(3)+' ms':n.toFixed(2)+' µs';
function color(s){let h=0;for(const c of s)h=(h*31+c.charCodeAt(0))|0;return `hsl(${(h>>>0)%360} 48% 47%)`;}
function windowTo(a,b){const span=Math.min(data.duration,Math.max(0.01,b-a));lo=Math.max(0,Math.min(a,data.duration-span));hi=lo+span;hideTip();hoverTime=null;drawSoon();}
function zoom(f,at=(lo+hi)/2){windowTo(at+(lo-at)*f,at+(hi-at)*f);}
function drawSoon(){if(!pending){pending=true;requestAnimationFrame(()=>{pending=false;draw();});}}
function lower(a,t){let l=0,r=a.length;while(l<r){const m=(l+r)>>1;if(a[m][0]<t)l=m+1;else r=m;}return l;}
function canvasContext(canvas,height){const w=canvas.parentElement.clientWidth,dpr=devicePixelRatio||1;canvas.width=w*dpr;canvas.height=height*dpr;canvas.style.height=height+'px';const ctx=canvas.getContext('2d');ctx.scale(dpr,dpr);ctx.font='10px system-ui';return [ctx,w];}
function ticks(){const raw=(hi-lo)/8,pow=10**Math.floor(Math.log10(raw));return [1,2,5,10].find(n=>n*pow>=raw)*pow;}
function grid(ctx,w,h){const step=ticks();ctx.strokeStyle='#293345';ctx.lineWidth=1;for(let t=Math.ceil(lo/step)*step;t<=hi;t+=step){const x=(t-lo)/(hi-lo)*w;ctx.beginPath();ctx.moveTo(x,0);ctx.lineTo(x,h);ctx.stroke();}}
function counterRange(points){let min=0,max=0;for(const p of points){min=Math.min(min,p[1]);max=Math.max(max,p[1]);}return {min,max};}
function draw(){
 const [rc,rw]=canvasContext($('ruler').querySelector('canvas'),24);grid(rc,rw,24);rc.fillStyle='#bdcce0';const step=ticks();for(let t=Math.ceil(lo/step)*step;t<=hi;t+=step)rc.fillText(time(t),(t-lo)/(hi-lo)*rw+4,16);
 let visible=0;
 for(const row of rows){if(row.element.hidden)continue;visible++;const {track,canvas}=row,h=track.kind==='counter'?(track.height??66):track.depth*21+6;const [ctx,w]=canvasContext(canvas,h);grid(ctx,w,h);const x=t=>(t-lo)/(hi-lo)*w;row.hits=[];
  if(track.kind==='slice'){
   for(const item of track.items){if(item.start>hi)break;if(item.end<lo)continue;const left=Math.max(0,x(item.start)),right=Math.min(w,x(item.end)),y=3+item.depth*21,width=Math.max(2,right-left);
    ctx.fillStyle=item.incomplete?'#795d4a':color(item.name);ctx.fillRect(left,y,width,18);if(width>25){ctx.save();ctx.beginPath();ctx.rect(left,y,width,18);ctx.clip();ctx.fillStyle='#fff';ctx.fillText(item.name,left+4,y+13);ctx.restore();}row.hits.push({left,right:left+width,y,item});}
  }else{
   row.counterScale=null;
   const bars=track.render==='bars',points=track.items,start=Math.max(0,lower(points,lo)-(bars?0:1)),end=Math.min(points.length,lower(points,hi)+1),view=points.slice(start,end).filter(p=>!bars||p[0]<=hi);
   const {min,max}=row.counterRange??=counterRange(points);
   row.counterScale={min,max:min===max?1:max,height:h};const y=v=>counterY(v,row.counterScale);
   ctx.save();ctx.beginPath();ctx.rect(0,0,w,h);ctx.clip();const counterColor=color(track.counterName);ctx.fillStyle=counterColor;
   if(bars){for(const p of view)ctx.fillRect(x(p[0])-1.5,Math.min(h-1,y(0),y(p[1])),3,Math.max(1,Math.abs(y(p[1])-y(0))));}
   else if(view.length){const line=new Path2D();for(let i=0;i<view.length;i++){const p=view[i];if(!i)line.moveTo(x(p[0]),y(p[1]));else{if(track.mode==='step')line.lineTo(x(p[0]),y(view[i-1][1]));line.lineTo(x(p[0]),y(p[1]));}}
   let lastX=x(view[view.length-1][0]);if(track.mode==='step'&&end===points.length){lastX=x(hi);line.lineTo(lastX,y(points[points.length-1][1]));}
   const area=new Path2D(line);area.lineTo(lastX,y(0));area.lineTo(x(view[0][0]),y(0));area.closePath();ctx.globalAlpha=0.2;ctx.fill(area);ctx.globalAlpha=1;ctx.strokeStyle=counterColor;ctx.lineWidth=1.5;ctx.stroke(line);for(const p of view)ctx.fillRect(x(p[0])-1.5,y(p[1])-1.5,3,3);}
   ctx.restore();ctx.fillStyle='#bfcede';ctx.fillText(max.toLocaleString(),5,12);
  }
  if(hoverTime!==null){ctx.strokeStyle='#e1e8f0';ctx.setLineDash([3,3]);ctx.beginPath();ctx.moveTo(x(hoverTime),0);ctx.lineTo(x(hoverTime),h);ctx.stroke();ctx.setLineDash([]);}
 }
 if($('trackCount'))$('trackCount').textContent=`${visible} / ${rows.length} tracks`;
 $('timeRange').textContent=`${time(lo)} — ${time(hi)} [${time(hi-lo)}]`.replaceAll(' ','').replace('—',' – ').replace('[',' [');
}
function sliceHit(hits,px,py){
 let nearest=null,distance=Infinity;
 for(const h of hits){if(py<h.y||py>h.y+18)continue;const gap=Math.max(h.left-px,px-h.right,0);if(gap<=4&&gap<distance){nearest=h.item;distance=gap;}}
 return nearest;
}
function hit(row,event){const rect=row.canvas.getBoundingClientRect(),px=event.clientX-rect.left,py=event.clientY-rect.top;
 if(row.track.kind==='slice')return sliceHit(row.hits,px,py);
 const point=sampleNear(row,event);return point?{start:point[0],end:point[0],value:point[1]}:null;
}
function sampleNear(row,e){const b=row.canvas.getBoundingClientRect(),t=lo+(e.clientX-b.left)/b.width*(hi-lo),a=row.track.items;let i=lower(a,t);if(i===a.length)i--;else if(i>0&&Math.abs(a[i-1][0]-t)<Math.abs(a[i][0]-t))i--;const point=a[i];return point&&point[0]>=lo&&point[0]<=hi&&Math.abs(point[0]-t)/(hi-lo)*b.width<=4?point:null;}
function sampleAt(points,t){const i=lower(points,t);return i<points.length&&points[i][0]===t?points[i][1]:null;}
function counterY(value,{min,max,height}){return height-(value-min)/(max-min)*(height-18);}
function counterLineValue(row,e){
 if(row.track.render==='bars'||!row.counterScale)return null;
 const b=row.canvas.getBoundingClientRect(),t=lo+(e.clientX-b.left)/b.width*(hi-lo),points=row.track.items,i=lower(points,t);
 if(t<lo||t>hi||!points.length||t<points[0][0])return null;
 let value;
 if(i<points.length&&points[i][0]===t)value=points[i][1];
 else if(row.track.mode==='step')value=points[i-1][1];
 else if(i===points.length)return null;
 else{const a=points[i-1],z=points[i];value=a[1]+(z[1]-a[1])*(t-a[0])/(z[0]-a[0]);}
 const y=counterY(value,row.counterScale);
 return Math.abs(e.clientY-b.top-y)<=4?value:null;
}
function visibleCounters(){const viewport=$('viewer').getBoundingClientRect(),top=$('ruler').getBoundingClientRect().bottom;return rows.filter(r=>{if(r.element.hidden||r.track.kind!=='counter')return false;const b=r.canvas.getBoundingClientRect();return b.bottom>top&&b.top<viewport.bottom;});}
function hideTip(){tip.style.display='none';for(const r of rows)if(r.counterTip)r.counterTip.style.display='none';}
function sliceTooltip(item,e){tip.textContent=JSON.stringify(describe(item),null,2);tip.style.display='block';positionTip(e);}
function positionTip(e){const b=tip.getBoundingClientRect();tip.style.left=Math.max(4,Math.min(e.clientX+12,innerWidth-b.width-8))+'px';tip.style.top=Math.max(4,Math.min(e.clientY+12,innerHeight-b.height-8))+'px';}
function counterTooltip(row,e){
 hideTip();const point=sampleNear(row,e);hoverTime=point?point[0]:null;if(!point){
  const value=counterLineValue(row,e);if(value!==null){tip.textContent=String(Number(value.toPrecision(8)));tip.style.display='block';positionTip(e);}
  drawSoon();return;
 }
 const viewport=$('viewer').getBoundingClientRect(),top=$('ruler').getBoundingClientRect().bottom;
 for(const r of visibleCounters()){
  const value=sampleAt(r.track.items,hoverTime);if(value===null)continue;
  if(!r.counterTip){r.counterTip=document.createElement('div');r.counterTip.className='counter-tip';document.body.append(r.counterTip);}
  const box=r.counterTip,b=r.canvas.getBoundingClientRect();box.textContent=String(value);box.style.display='block';
  const width=box.offsetWidth,height=box.offsetHeight,x=b.left+(hoverTime-lo)/(hi-lo)*b.width;
  box.style.left=Math.max(b.left,Math.min(x+8,b.right-width-4))+'px';
  box.style.top=Math.max(top,Math.min(Math.max(b.top,top)+8,b.bottom-height,viewport.bottom-height))+'px';
 }
 drawSoon();
}
function describe(item){
 if(item.value!==undefined)return {value:item.value};
 const details={name:item.name,duration:time(item.end-item.start)};
 if(item.category)details.category=item.category;
 if(Object.keys(item.args||{}).length)details.args=item.args;
 if(Object.keys(item.endArgs||{}).length)details.endArgs=item.endArgs;
 return details;
}
function navigateWheel(e,canvas){
 const rect=canvas.getBoundingClientRect(),unit=e.deltaMode===1?16:e.deltaMode===2?$('viewer').clientHeight:1;
 if(e.ctrlKey||e.metaKey){e.preventDefault();const delta=(e.deltaY||e.deltaX)*unit;zoom(Math.exp(Math.max(-1,Math.min(1,delta*0.002))),lo+(e.clientX-rect.left)/rect.width*(hi-lo));}
 else if(e.deltaX){e.preventDefault();const shift=e.deltaX*unit/rect.width*(hi-lo);windowTo(lo+shift,hi+shift);if(e.deltaY)$('viewer').scrollTop+=e.deltaY*unit;}
}
function focus(item){if(item&&item.end>item.start){const pad=(item.end-item.start)*0.1;windowTo(item.start-pad,item.end+pad);}}
for(const track of data.tracks){const element=document.createElement('div');element.className='row';const label=document.createElement('div');label.className='label';label.textContent=track.name;const sub=document.createElement('small');sub.textContent=`${track.kind} · ${track.items.length.toLocaleString()} ${track.kind==='slice'?'events':'samples'} · pid ${track.pid}`;label.append(sub);const plot=document.createElement('div');plot.className='plot';const canvas=document.createElement('canvas');plot.append(canvas);element.append(label,plot);$('tracks').append(element);const row={track,element,canvas,hits:[]};rows.push(row);
 canvas.addEventListener('wheel',e=>navigateWheel(e,canvas),{passive:false});
 canvas.addEventListener('pointerdown',e=>{if(e.button!==0)return;$('viewer').focus({preventScroll:true});drag={x:e.clientX,lo,hi};canvas.setPointerCapture(e.pointerId);});
 canvas.addEventListener('pointermove',e=>{if(drag){const dx=e.clientX-drag.x;const shift=-dx/canvas.clientWidth*(drag.hi-drag.lo);windowTo(drag.lo+shift,drag.hi+shift);return;}if(track.kind==='counter'){counterTooltip(row,e);return;}hideTip();hoverTime=null;const item=hit(row,e);tip.style.display=item?'block':'none';if(item){sliceTooltip(item,e);}drawSoon();});
 canvas.addEventListener('pointerup',()=>{drag=null;});
 canvas.addEventListener('pointercancel',()=>{drag=null;});canvas.addEventListener('pointerleave',()=>{hideTip();hoverTime=null;drawSoon();});canvas.addEventListener('dblclick',e=>focus(hit(row,e)));
}
function applyFilters(){hideTip();hoverTime=null;const terms=id=>($(id)?.value||'').toLowerCase().split(',').map(s=>s.trim()).filter(Boolean),includes=terms('filter'),excludes=terms('exclude');for(const r of rows){const name=r.track.name.toLowerCase();r.element.hidden=(includes.length>0&&!includes.some(t=>name.includes(t)))||excludes.some(t=>name.includes(t));}drawSoon();}
$('filter')?.addEventListener('input',applyFilters);$('exclude')?.addEventListener('input',applyFilters);
$('fit').onclick=()=>windowTo(0,data.duration);$('zoomIn').onclick=()=>zoom(0.5);$('zoomOut').onclick=()=>zoom(2);
$('viewer').addEventListener('keydown',e=>{if(e.key==='+'||e.key==='=')zoom(0.5);else if(e.key==='-')zoom(2);else if(e.key==='ArrowLeft')windowTo(lo-(hi-lo)*0.2,hi-(hi-lo)*0.2);else if(e.key==='ArrowRight')windowTo(lo+(hi-lo)*0.2,hi+(hi-lo)*0.2);else if(e.key==='Escape'){if(tip.style.display==='block'||hoverTime!==null){hideTip();hoverTime=null;drawSoon();}else windowTo(0,data.duration);}else return;e.preventDefault();});
$('viewer').addEventListener('scroll',()=>{hideTip();hoverTime=null;drawSoon();});
new ResizeObserver(()=>{hideTip();hoverTime=null;drawSoon();}).observe($('viewer'));drawSoon();
</script></html>'''


def render(data, title, *, fixed_filter=False, fixed_exclude=False):
    # Escape '<' so trace names/arguments cannot terminate the embedded script.
    payload = json.dumps(data, ensure_ascii=True, separators=(",", ":"), allow_nan=False).replace("<", "\\u003c")
    controls = {
        "__TRACK_COUNT__": "" if fixed_filter and fixed_exclude else '<span id="trackCount"></span>',
        "__FILTER__": "" if fixed_filter else '<input id="filter" placeholder="Filter tracks (comma-separated OR)" aria-label="Filter tracks">',
        "__EXCLUDE__": "" if fixed_exclude else '<input id="exclude" placeholder="Exclude tracks (comma-separated OR)" aria-label="Exclude tracks">',
    }
    output = TEMPLATE
    for marker, control in controls.items():
        output = output.replace(marker, control)
    return output.replace("__TITLE__", html.escape(title)).replace("__DATA__", payload)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace", type=Path, help="Chrome Trace JSON (.json or .json.gz); timestamps in microseconds")
    parser.add_argument("-o", "--output", type=Path, help="HTML output (default: input basename with .html in the current directory)")
    parser.add_argument("--include", action="append", default=[], metavar="NAME", help="Include track names containing NAME (case-insensitive); repeat for OR")
    parser.add_argument("--exclude", action="append", default=[], metavar="NAME", help="Exclude track names containing NAME (case-insensitive); repeat for OR; overrides includes")
    parser.add_argument("--preset", choices=PRESETS, default="gc", help="Counter style preset (default: gc)")
    parser.add_argument("--counter-height", type=counter_height, default=66, metavar="PX", help="Counter track height, 40..600 pixels (default: 66)")
    parser.add_argument("--style", type=style_rule, action="append", default=[], metavar="RULE", help="Track style: 'GLOB: render=line|bars mode=linear|step height=PX order=N'; repeat, later properties override")
    parser.add_argument("--title", help="Page title (default: input filename)")
    args = parser.parse_args()
    if args.output is None:
        basename = Path(args.trace.name)
        if basename.suffix == ".gz":
            basename = basename.with_suffix("")
        args.output = basename.with_suffix(".html")
    try:
        if args.trace.resolve() == args.output.resolve():
            raise ValueError("Input and output must be different files")
        opener = gzip.open if args.trace.suffix == ".gz" else open
        with opener(args.trace, "rt", encoding="utf-8") as source:
            data = prepare(json.load(source), args.include, args.exclude)
        if not data["tracks"]:
            raise ValueError("No supported tracks matched the filters")
        customize(data, args.preset, args.counter_height, args.style)
        args.output.write_text(render(data, args.title or args.trace.name,
                                      fixed_filter=bool(args.include), fixed_exclude=bool(args.exclude)), encoding="utf-8")
    except (OSError, ValueError, TypeError) as error:
        parser.exit(1, f"error: {error}\n")
    for warning in data["warnings"]:
        print(f"warning: {warning}", file=sys.stderr)
    print(f"Wrote {args.output}: {len(data['tracks'])} tracks, {sum(len(t['items']) for t in data['tracks'])} events/samples")


if __name__ == "__main__":
    main()
