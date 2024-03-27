import {expect, test} from 'vitest';
import {InnerJoinOperator} from './join-operator.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {NoOp} from './operator.js';
import {Multiset} from '../../multiset.js';
import {JoinResult, joinSymbol} from '../../types.js';

type Track = {
  id: number;
  title: string;
  length: number;
  albumId: number;
};

type Album = {
  id: number;
  title: string;
};

type TrackArtist = {
  trackId: number;
  artistId: number;
};

type Artist = {
  id: number;
  name: string;
};

type Playlist = {
  id: number;
  title: string;
};

type PlaylistTrack = {
  playlistId: number;
  trackId: number;
};

test('unbalanced input', () => {
  const trackWriter = new DifferenceStreamWriter<Track>();
  const albumWriter = new DifferenceStreamWriter<Album>();
  const trackReader = trackWriter.newReader();
  const albumReader = albumWriter.newReader();
  const output = new DifferenceStreamWriter<
    JoinResult<Track, Album, 'track', 'album'>
  >();

  new InnerJoinOperator<number, Track, Album, 'track', 'album'>({
    a: trackReader,
    aAs: 'track',
    getAJoinKey: track => track.albumId,
    getAPrimaryKey: track => track.id,
    b: albumReader,
    bAs: 'album',
    getBJoinKey: album => album.id,
    getBPrimaryKey: album => album.id,
    output,
  });

  const outReader = output.newReader();
  outReader.setOperator(new NoOp());

  trackWriter.queueData([
    1,
    new Multiset([
      [
        {
          id: 1,
          title: 'Track One',
          length: 1,
          albumId: 1,
        },
        1,
      ],
    ]),
  ]);

  trackWriter.notify(1);
  trackWriter.notifyCommitted(1);
  const entry = outReader.drain(1);
  expect([...entry![1].entries]).toEqual([]);

  // now add to the other side
  albumWriter.queueData([
    2,
    new Multiset([
      [
        {
          id: 1,
          title: 'Album One',
        },
        1,
      ],
    ]),
  ]);

  albumWriter.notify(2);
  albumWriter.notifyCommitted(2);
  const entry2 = outReader.drain(2);
  expect([...entry2![1].entries]).toEqual([
    [
      {
        id: '1_1',
        [joinSymbol]: true,
        track: {
          id: 1,
          title: 'Track One',
          length: 1,
          albumId: 1,
        },
        album: {
          id: 1,
          title: 'Album One',
        },
      },
      1,
    ],
  ]);

  // now try deleting items
  // and see that we get the correct new join result
});

// try unbalanced in the opposite direction

test('balanced input', () => {});

test('basic join', () => {
  const trackWriter = new DifferenceStreamWriter<Track>();
  const albumWriter = new DifferenceStreamWriter<Album>();
  const trackReader = trackWriter.newReader();
  const albumReader = albumWriter.newReader();
  const output = new DifferenceStreamWriter<
    JoinResult<Track, Album, 'track', 'album'>
  >();

  new InnerJoinOperator<number, Track, Album, 'track', 'album'>({
    a: trackReader,
    aAs: 'track',
    getAJoinKey: track => track.albumId,
    getAPrimaryKey: track => track.id,
    b: albumReader,
    bAs: 'album',
    getBJoinKey: album => album.id,
    getBPrimaryKey: album => album.id,
    output,
  });

  const outReader = output.newReader();
  outReader.setOperator(new NoOp());

  trackWriter.queueData([
    1,
    new Multiset([
      [
        {
          id: 1,
          title: 'Track One',
          length: 1,
          albumId: 1,
        },
        1,
      ],
    ]),
  ]);

  albumWriter.queueData([
    1,
    new Multiset([
      [
        {
          id: 1,
          title: 'Album One',
        },
        1,
      ],
    ]),
  ]);

  check([
    [
      {
        id: '1_1',
        [joinSymbol]: true,
        track: {
          id: 1,
          title: 'Track One',
          length: 1,
          albumId: 1,
        },
        album: {
          id: 1,
          title: 'Album One',
        },
      },
      1,
    ],
  ]);

  function check(expected: [unknown, number][]) {
    trackWriter.notify(1);
    albumWriter.notify(1);
    trackWriter.notifyCommitted(1);
    albumWriter.notifyCommitted(1);
    const entry = outReader.drain(1);
    expect([...entry![1].entries]).toEqual(expected);
  }
});

test('join through a junction table', () => {
  // track -> track_artist -> artist
  const trackWriter = new DifferenceStreamWriter<Track>();
  const trackArtistWriter = new DifferenceStreamWriter<TrackArtist>();
  const artistWriter = new DifferenceStreamWriter<Artist>();
  const trackReader = trackWriter.newReader();
  const trackArtistReader = trackArtistWriter.newReader();
  const artistReader = artistWriter.newReader();

  const trackAndTrackArtistOutput = new DifferenceStreamWriter<
    JoinResult<Track, TrackArtist, 'track', 'trackArtist'>
  >();

  new InnerJoinOperator<number, Track, TrackArtist, 'track', 'trackArtist'>({
    a: trackReader,
    aAs: 'track',
    getAJoinKey: track => track.id,
    getAPrimaryKey: track => track.id,
    b: trackArtistReader,
    bAs: 'trackArtist',
    getBJoinKey: trackArtist => trackArtist.trackId,
    getBPrimaryKey: trackArtist =>
      trackArtist.trackId + '-' + trackArtist.artistId,
    output: trackAndTrackArtistOutput,
  });

  const output = new DifferenceStreamWriter<
    JoinResult<
      JoinResult<Track, TrackArtist, 'track', 'trackArtist'>,
      Artist,
      undefined,
      'artist'
    >
  >();

  new InnerJoinOperator<
    number,
    JoinResult<Track, TrackArtist, 'track', 'trackArtist'>,
    Artist,
    undefined,
    'artist'
  >({
    a: trackAndTrackArtistOutput.newReader(),
    aAs: undefined,
    getAJoinKey: x => x.trackArtist.artistId,
    getAPrimaryKey: x => x.id,
    b: artistReader,
    bAs: 'artist',
    getBJoinKey: x => x.id,
    getBPrimaryKey: x => x.id,
    output,
  });
  const outReader = output.newReader();
  outReader.setOperator(new NoOp());

  trackWriter.queueData([
    1,
    new Multiset([
      [
        {
          id: 1,
          title: 'Track One',
          length: 1,
          albumId: 1,
        },
        1,
      ],
    ]),
  ]);
  trackArtistWriter.queueData([
    1,
    new Multiset([
      [
        {
          trackId: 1,
          artistId: 1,
        },
        1,
      ],
      [
        {
          trackId: 1,
          artistId: 2,
        },
        1,
      ],
    ]),
  ]);
  artistWriter.queueData([
    1,
    new Multiset([
      [
        {
          id: 1,
          name: 'Artist One',
        },
        1,
      ],
      [
        {
          id: 2,
          name: 'Artist Two',
        },
        1,
      ],
    ]),
  ]);

  trackWriter.notify(1);
  trackArtistWriter.notify(1);
  artistWriter.notify(1);
  trackWriter.notifyCommitted(1);
  trackArtistWriter.notifyCommitted(1);
  artistWriter.notifyCommitted(1);

  const entry = outReader.drain(1);
  expect([...entry![1].entries]).toEqual([
    [
      {
        id: '1_1-1_1',
        track: {id: 1, title: 'Track One', length: 1, albumId: 1},
        trackArtist: {trackId: 1, artistId: 1},
        artist: {id: 1, name: 'Artist One'},
        [joinSymbol]: true,
      },
      1,
    ],
    [
      {
        id: '1_1-2_2',
        track: {id: 1, title: 'Track One', length: 1, albumId: 1},
        trackArtist: {trackId: 1, artistId: 2},
        artist: {id: 2, name: 'Artist Two'},
        [joinSymbol]: true,
      },
      1,
    ],
  ]);

  // remove an artist

  // remove a track-artist link

  // remove everything
});

/*
fast-check join...

1. create tables
2. join in different directions
3. add items

*/

test('join followed by reduction to gather playlists', () => {
  /**
   * For a user:
   * - Join their playlists
   * - Join their tracks
   * - Join their albums
   * - Join their atrists
   *
   */
});
