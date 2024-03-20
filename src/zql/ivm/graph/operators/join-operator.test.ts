import {expect, test} from 'vitest';
import {InnerJoinOperator} from './join-operator.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {NoOp} from './operator.js';
import {Multiset} from '../../multiset.js';

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

type TrackAndAlbum = {
  track: Track;
  album: Album;
};

type TrackAndTrackArtistAndArtist = {
  track: Track;
  trackArtist: TrackArtist;
  artist: Artist;
};

test('unbalanced input', () => {
  const trackWriter = new DifferenceStreamWriter<Track>();
  const albumWriter = new DifferenceStreamWriter<Album>();
  const trackReader = trackWriter.newReader();
  const albumReader = albumWriter.newReader();
  const output = new DifferenceStreamWriter<TrackAndAlbum>();

  new InnerJoinOperator<number, Track, Album, 'track', 'album'>(
    trackReader,
    'track',
    track => track.albumId,
    track => track.id,
    albumReader,
    'album',
    album => album.id,
    album => album.id,
    output,
  );

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
  const items = outReader.drain(1);
  expect(items.length).toBe(1);
  const entry = items[0];
  expect([...entry[1].entries]).toEqual([]);

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
  const items2 = outReader.drain(2);
  expect(items2.length).toBe(1);
  const entry2 = items2[0];
  expect([...entry2[1].entries]).toEqual([
    [
      {
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
  const output = new DifferenceStreamWriter<TrackAndAlbum>();

  new InnerJoinOperator<number, Track, Album, 'track', 'album'>(
    trackReader,
    'track',
    track => track.albumId,
    track => track.id,
    albumReader,
    'album',
    album => album.id,
    album => album.id,
    output,
  );

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

  function check(expected: [TrackAndAlbum, number][]) {
    trackWriter.notify(1);
    albumWriter.notify(1);
    trackWriter.notifyCommitted(1);
    albumWriter.notifyCommitted(1);
    const items = outReader.drain(1);
    expect(items.length).toBe(1);
    const entry = items[0];
    expect([...entry[1].entries]).toEqual(expected);
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

  const output = new DifferenceStreamWriter<TrackAndTrackArtistAndArtist>();
});

test('complicated join', () => {
  /**
   * For a user:
   * - Join their playlists
   * - Join their tracks
   * - Join their albums
   * - Join their atrists
   *
   */
});
